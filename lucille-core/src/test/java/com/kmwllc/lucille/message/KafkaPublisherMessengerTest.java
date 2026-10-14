package com.kmwllc.lucille.message;

import com.kmwllc.lucille.connector.SequenceConnector;
import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.ConnectorResult;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.core.Runner;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class KafkaPublisherMessengerTest {

  /**
   * Test that a Kafka send failure in distributed mode (reported asynchronously via producer
   * callback) causes the run to abort promptly on the next publish call, rather than hanging
   * until the connector timeout.
   *
   * This exercises the deferred-error path in KafkaPublisherMessenger: the callback captures the
   * exception, and checkException() surfaces it on the next call to sendForProcessing().
   *
   * See RunnerTest.testPublishFailureAbortsRunLocal for the local-mode equivalent where
   * sendForProcessing() fails immediately from an in-memory queue.
   */
  @Test
  public void testPublishFailureAbortsRunDistributed() throws Exception {
    Config connectorConfig = ConfigFactory.parseMap(Map.of(
        "numDocs", 10,
        "name", "connector1",
        "class", "com.kmwllc.lucille.connector.SequenceConnector",
        "pipeline", "pipeline1"
    ));

    Config runnerConfig = ConfigFactory.parseMap(Map.of(
        "runner.connectorTimeout", 30000
    ));

    // Mock the KafkaProducer: first 2 sends succeed (callback with null exception),
    // 3rd send fails (callback with an exception)
    KafkaProducer<String, Document> mockProducer = mock(KafkaProducer.class);
    AtomicInteger sendCount = new AtomicInteger(0);

    RecordMetadata dummyMetadata = new RecordMetadata(new TopicPartition("test", 0), 0, 0, 0L, 0, 0);
    Future<RecordMetadata> mockFuture = mock(Future.class);
    when(mockFuture.get()).thenReturn(dummyMetadata);

    when(mockProducer.send(any(ProducerRecord.class), any(Callback.class))).thenAnswer(invocation -> {
      Callback callback = invocation.getArgument(1);
      int count = sendCount.incrementAndGet();
      if (count >= 3) {
        // simulate async failure: invoke callback with an exception
        callback.onCompletion(null, new Exception("Simulated broker failure"));
      } else {
        // simulate success
        callback.onCompletion(dummyMetadata, null);
      }
      return mockFuture;
    });

    // Mock event consumer to return empty records (no events to process)
    @SuppressWarnings("unchecked")
    KafkaConsumer<String, String> mockEventConsumer = mock(KafkaConsumer.class);
    when(mockEventConsumer.poll(any())).thenReturn(ConsumerRecords.empty());

    // Use MockedStatic to intercept KafkaUtils calls made during initialize()
    Config messengerConfig = ConfigFactory.parseMap(Map.of(
        "kafka.sourceTopic", "test_source"
    ));

    try (MockedStatic<KafkaUtils> kafkaUtils = mockStatic(KafkaUtils.class)) {
      kafkaUtils.when(() -> KafkaUtils.createEventTopic(any(), any(), any())).thenAnswer(inv -> null);
      kafkaUtils.when(() -> KafkaUtils.getEventTopicName(any(), any(), any())).thenReturn("test_events");
      kafkaUtils.when(() -> KafkaUtils.createEventConsumer(any(), any())).thenReturn(mockEventConsumer);
      kafkaUtils.when(() -> KafkaUtils.createDocumentProducer(any())).thenReturn(mockProducer);
      kafkaUtils.when(() -> KafkaUtils.getSourceTopicName(any(), any())).thenReturn("test_source");

      KafkaPublisherMessenger messenger = new KafkaPublisherMessenger(messengerConfig);

      Connector connector = new SequenceConnector(connectorConfig);
      PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

      Instant start = Instant.now();
      ConnectorResult result = Runner.runConnector(runnerConfig, "run1", connector, publisher);
      Instant end = Instant.now();

      // run aborted
      assertFalse(result.getStatus());

      // run completed promptly — did not hang for the full 30s connector timeout
      assertTrue("Run took too long — may have hung instead of aborting",
          ChronoUnit.SECONDS.between(start, end) < 10);

      // numPublished is 3, not 2: the 3rd send's failure is reported via callback after
      // sendForProcessing() has already returned, so PublisherImpl considers it published.
      // This is a consequence of the async design — numPublished reflects documents handed
      // to the producer, not documents confirmed by the broker.
      assertEquals(3, publisher.numPublished());

      // the failure is deferred: sends 1-2 succeed, send 3 fails via callback, and the exception
      // is surfaced on send 4's checkException() call. So at least 3 sends were attempted.
      // (We can't assert on docs sent for processing as with TestMessenger — the mock producer's
      // send count serves the same purpose here.)
      verify(mockProducer, atLeast(3)).send(any(ProducerRecord.class), any(Callback.class));
    }
  }

  /**
   * Test that when the final document published by the connector fails asynchronously (no
   * subsequent sendForProcessing call to surface it via checkException), the error is caught
   * by publisher.flush() at end of connector execution, and the run still aborts rather than
   * hanging.
   *
   * The chain is: ConnectorThread calls PublisherImpl.flush(), which calls
   * KafkaPublisherMessenger.flush(), which calls kafkaProducer.flush() (a no-op on the mock
   * since callbacks have already fired synchronously) and then checkException(), which finds
   * the exception captured by the send callback and throws. An exception thrown directly by
   * kafkaProducer.flush() itself (e.g. producer closed, interrupted) would propagate through
   * the same code path but is not simulated here.
   *
   * This is the specific edge case that motivated adding flush() to the PublisherMessenger
   * interface: without it, the failed docId would be orphaned in docIdsToTrack and
   * waitForCompletion() would hang until the connector timeout.
   */
  @Test
  public void testFlushSurfacesLastDocFailure() throws Exception {
    int numDocs = 5;

    Config connectorConfig = ConfigFactory.parseMap(Map.of(
        "numDocs", numDocs,
        "name", "connector1",
        "class", "com.kmwllc.lucille.connector.SequenceConnector",
        "pipeline", "pipeline1"
    ));

    Config runnerConfig = ConfigFactory.parseMap(Map.of(
        "runner.connectorTimeout", 30000
    ));

    // Mock the KafkaProducer: all sends succeed except the last one
    KafkaProducer<String, Document> mockProducer = mock(KafkaProducer.class);
    AtomicInteger sendCount = new AtomicInteger(0);

    RecordMetadata dummyMetadata = new RecordMetadata(new TopicPartition("test", 0), 0, 0, 0L, 0, 0);
    Future<RecordMetadata> mockFuture = mock(Future.class);
    when(mockFuture.get()).thenReturn(dummyMetadata);

    when(mockProducer.send(any(ProducerRecord.class), any(Callback.class))).thenAnswer(invocation -> {
      Callback callback = invocation.getArgument(1);
      int count = sendCount.incrementAndGet();
      if (count == numDocs) {
        // last send fails via callback
        callback.onCompletion(null, new Exception("Simulated broker failure on last doc"));
      } else {
        callback.onCompletion(dummyMetadata, null);
      }
      return mockFuture;
    });

    @SuppressWarnings("unchecked")
    KafkaConsumer<String, String> mockEventConsumer = mock(KafkaConsumer.class);
    when(mockEventConsumer.poll(any())).thenReturn(ConsumerRecords.empty());

    Config messengerConfig = ConfigFactory.parseMap(Map.of(
        "kafka.sourceTopic", "test_source"
    ));

    try (MockedStatic<KafkaUtils> kafkaUtils = mockStatic(KafkaUtils.class)) {
      kafkaUtils.when(() -> KafkaUtils.createEventTopic(any(), any(), any())).thenAnswer(inv -> null);
      kafkaUtils.when(() -> KafkaUtils.getEventTopicName(any(), any(), any())).thenReturn("test_events");
      kafkaUtils.when(() -> KafkaUtils.createEventConsumer(any(), any())).thenReturn(mockEventConsumer);
      kafkaUtils.when(() -> KafkaUtils.createDocumentProducer(any())).thenReturn(mockProducer);
      kafkaUtils.when(() -> KafkaUtils.getSourceTopicName(any(), any())).thenReturn("test_source");

      KafkaPublisherMessenger messenger = new KafkaPublisherMessenger(messengerConfig);

      Connector connector = new SequenceConnector(connectorConfig);
      PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

      Instant start = Instant.now();
      ConnectorResult result = Runner.runConnector(runnerConfig, "run1", connector, publisher);
      Instant end = Instant.now();

      // run aborted
      assertFalse(result.getStatus());

      // run completed promptly — the flush caught the error, not the connector timeout
      assertTrue("Run took too long — flush may not have surfaced the error",
          ChronoUnit.SECONDS.between(start, end) < 10);

      // all sends were attempted — the failure only surfaces at flush time
      assertEquals(numDocs, sendCount.get());

      // numPublished is numDocs because the last send's failure is deferred past the
      // numPublished increment
      assertEquals(numDocs, publisher.numPublished());
    }
  }
}
