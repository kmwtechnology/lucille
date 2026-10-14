package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.KafkaDocument;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class KafkaIndexerMessengerTest {

  /**
   * Test that when one completion event in a batch fails to send (i.e. the Kafka producer
   * reports failure via callback for one of the FINISH events confirming that a document was
   * successfully indexed), sendEvents() still attempts all sends in the batch, flushes, and
   * then throws an exception indicating the failure.
   */
  @Test
  public void testSendEventsPartialFailure() throws Exception {
    KafkaProducer<String, String> mockEventProducer = mock(KafkaProducer.class);
    AtomicInteger sendCount = new AtomicInteger(0);

    RecordMetadata dummyMetadata = new RecordMetadata(new TopicPartition("test", 0), 0, 0, 0L, 0, 0);

    // 5 documents in the batch: send #3 fails, all others succeed
    when(mockEventProducer.send(any(ProducerRecord.class), any(Callback.class))).thenAnswer(invocation -> {
      Callback callback = invocation.getArgument(1);
      int count = sendCount.incrementAndGet();
      if (count == 3) {
        callback.onCompletion(null, new Exception("Simulated event send failure"));
      } else {
        callback.onCompletion(dummyMetadata, null);
      }
      return null;
    });

    @SuppressWarnings("unchecked")
    KafkaConsumer<String, KafkaDocument> mockDestConsumer = mock(KafkaConsumer.class);

    Config config = ConfigFactory.parseMap(Map.of(
        "kafka.bootstrapServers", "localhost:9092",
        "kafka.consumerGroupId", "test-group",
        "kafka.maxPollIntervalSecs", 300,
        "kafka.maxRequestSize", 1048576
    ));

    try (MockedStatic<KafkaUtils> kafkaUtils = mockStatic(KafkaUtils.class)) {
      kafkaUtils.when(() -> KafkaUtils.createDocumentConsumer(any(), any())).thenReturn(mockDestConsumer);
      kafkaUtils.when(() -> KafkaUtils.createEventProducer(any())).thenReturn(mockEventProducer);
      kafkaUtils.when(() -> KafkaUtils.getDestTopicName(any())).thenReturn("test_dest");
      kafkaUtils.when(() -> KafkaUtils.getEventTopicName(any(), any(), any())).thenReturn("test_events");

      KafkaIndexerMessenger messenger = new KafkaIndexerMessenger(config, "pipeline1");

      // Create 5 documents for the batch
      List<Document> docs = List.of(
          Document.create("doc1", "run1"),
          Document.create("doc2", "run1"),
          Document.create("doc3", "run1"),
          Document.create("doc4", "run1"),
          Document.create("doc5", "run1")
      );

      // sendEvents should throw because one send failed
      Exception thrown = assertThrows(Exception.class, () ->
          messenger.sendEvents(docs, "SUCCEEDED", Event.Type.FINISH));

      assertTrue(thrown.getMessage().contains("Failed to send one or more events"));
      assertTrue(thrown.getCause().getMessage().contains("Simulated event send failure"));

      // all 5 sends were attempted despite the failure on #3
      assertEquals(5, sendCount.get());
      verify(mockEventProducer, times(5)).send(any(ProducerRecord.class), any(Callback.class));

      // flush was called once at the end
      verify(mockEventProducer, times(1)).flush();
    }
  }
}
