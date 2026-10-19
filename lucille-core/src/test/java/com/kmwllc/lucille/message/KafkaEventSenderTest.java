package com.kmwllc.lucille.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.KafkaDocument;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

public class KafkaEventSenderTest {

  private final Config config = ConfigFactory.empty();
  private MockedStatic<KafkaUtils> kafkaUtils;
  private KafkaProducer<String, String> producer;

  // callbacks of sends the mock producer has accepted but not yet completed
  private final List<Callback> pending = new ArrayList<>();
  // the outcome the mock producer reports, on flush(), for each pending send
  private Exception nextOutcome = null;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    producer = mock(KafkaProducer.class);
    when(producer.send(any(ProducerRecord.class), any(Callback.class))).thenAnswer(invocation -> {
      pending.add(invocation.getArgument(1));
      return null;
    });
    doAnswer(invocation -> {
      for (Callback callback : pending) {
        callback.onCompletion(null, nextOutcome);
      }
      pending.clear();
      return null;
    }).when(producer).flush();

    kafkaUtils = mockStatic(KafkaUtils.class);
    kafkaUtils.when(() -> KafkaUtils.createEventProducer(any())).thenReturn(producer);
    kafkaUtils.when(() -> KafkaUtils.getEventTopicName(any(), any(), any())).thenReturn("events");
  }

  @After
  public void tearDown() {
    kafkaUtils.close();
  }

  private static Event event(String docId) {
    return new Event(docId, "run1", null, Event.Type.CREATE);
  }

  @Test
  public void testSendDoesNotWaitForAck() {
    KafkaEventSender sender = new KafkaEventSender(config, "pipeline1");
    sender.send(event("a"));
    sender.send(event("b"));

    // handed to the producer, not waited on
    assertEquals(2, pending.size());
    verify(producer, never()).flush();
    verify(producer, never()).send(any(ProducerRecord.class));
  }

  @Test
  public void testFlushDeliversPendingEvents() throws Exception {
    KafkaEventSender sender = new KafkaEventSender(config, "pipeline1");
    sender.send(event("a"));
    sender.flush();

    assertTrue(pending.isEmpty());
    verify(producer).flush();
  }

  @Test
  public void testSendFailureIsReportedByNextFlushOnlyOnce() throws Exception {
    KafkaEventSender sender = new KafkaEventSender(config, "pipeline1");
    Exception cause = new Exception("simulated event send failure");
    nextOutcome = cause;
    sender.send(event("a"));
    sender.send(event("b"));

    Exception thrown = assertThrows(Exception.class, sender::flush);
    assertSame(cause, thrown.getCause());
    // the failed events' documents are named, so a caller can't misattribute the failure
    assertTrue(thrown.getMessage(), thrown.getMessage().contains("CREATE a") && thrown.getMessage().contains("CREATE b"));

    // a later flush with no new failures succeeds
    nextOutcome = null;
    sender.send(event("c"));
    sender.flush();
  }

  @Test
  public void testFlushWithNothingSentSkipsProducer() throws Exception {
    KafkaEventSender sender = new KafkaEventSender(config, "pipeline1");
    sender.flush();
    sender.send(event("a"));
    sender.flush();
    sender.flush();
    verify(producer, org.mockito.Mockito.times(1)).flush();
  }

  @Test
  public void testFailureRecordedBeforeInterruptedFlushIsNotReportedLater() throws Exception {
    KafkaEventSender sender = new KafkaEventSender(config, "pipeline1");
    doAnswer(invocation -> {
      for (Callback callback : pending) {
        callback.onCompletion(null, new Exception("simulated event send failure"));
      }
      pending.clear();
      throw new org.apache.kafka.common.errors.InterruptException(new InterruptedException());
    }).when(producer).flush();
    sender.send(event("a"));
    Exception thrown = assertThrows(org.apache.kafka.common.errors.InterruptException.class, sender::flush);
    assertEquals(1, thrown.getSuppressed().length);
    Thread.interrupted(); // InterruptException re-sets the interrupt flag

    // the failure was reported with the interrupted flush, not carried over to the next one; and since sends may
    // still be in flight after an interrupted flush, the next flush waits on the producer even with no new sends
    doAnswer(invocation -> null).when(producer).flush();
    sender.flush();
    verify(producer, org.mockito.Mockito.times(2)).flush();
  }

  @Test
  public void testDisabledSenderDoesNothing() throws Exception {
    kafkaUtils.when(() -> KafkaUtils.createEventProducer(any())).thenReturn(null);
    KafkaEventSender sender = new KafkaEventSender(config, "pipeline1");
    assertFalse(sender.isEnabled());
    sender.send(event("a"));
    sender.flush();
    sender.close();
  }

  @Test
  public void testHybridIndexerBatchCompleteFlushesBeforeQueuingOffsets() throws Exception {
    LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets = new LinkedBlockingQueue<>();
    HybridIndexerMessenger messenger = new HybridIndexerMessenger(config, new LinkedBlockingQueue<>(), offsets, null,
        "pipeline1");
    KafkaDocument doc = kafkaDoc("doc1", 4);

    // offsets are checked from inside flush(): they must not be queued until the events are delivered
    doAnswer(invocation -> {
      assertTrue(offsets.isEmpty());
      pending.clear();
      return null;
    }).when(producer).flush();

    messenger.sendEvent(doc, "FAILED", Event.Type.FAIL);
    assertEquals(1, pending.size());
    messenger.batchComplete(List.of(doc));

    verify(producer).flush();
    assertEquals(Map.of(new TopicPartition("source", 0), new OffsetAndMetadata(5)), offsets.poll());
  }

  @Test
  public void testHybridIndexerBatchCompleteQueuesOffsetsWhenFlushFails() throws Exception {
    LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets = new LinkedBlockingQueue<>();
    HybridIndexerMessenger messenger = new HybridIndexerMessenger(config, new LinkedBlockingQueue<>(), offsets, null,
        "pipeline1");
    KafkaDocument doc = kafkaDoc("doc1", 4);
    nextOutcome = new Exception("simulated event send failure");

    messenger.sendEvent(doc, "FAILED", Event.Type.FAIL);
    assertThrows(Exception.class, () -> messenger.batchComplete(List.of(doc)));

    // as when the FAIL event was sent synchronously, the failure is reported but does not hold back the offsets
    assertEquals(Map.of(new TopicPartition("source", 0), new OffsetAndMetadata(5)), offsets.poll());
  }

  @Test
  public void testKafkaIndexerBatchCompleteFlushesEvents() throws Exception {
    kafkaUtils.when(() -> KafkaUtils.createDocumentConsumer(any(), any()))
        .thenReturn(mock(org.apache.kafka.clients.consumer.KafkaConsumer.class));
    KafkaIndexerMessenger messenger = new KafkaIndexerMessenger(config, "pipeline1");
    Document doc = Document.create("doc1", "run1");
    nextOutcome = new Exception("simulated event send failure");

    messenger.sendEvent(doc, "FAILED", Event.Type.FAIL);
    assertEquals(1, pending.size());
    assertThrows(Exception.class, () -> messenger.batchComplete(List.of(doc)));
    assertTrue(pending.isEmpty());

    // close() also closes the event producer, delivering anything not yet flushed
    messenger.close();
    verify(producer).close();
  }

  private static KafkaDocument kafkaDoc(String id, long offset) throws Exception {
    String json = "{\"id\": \"" + id + "\", \"run_id\": \"run1\"}";
    return new KafkaDocument(new ConsumerRecord<>("source", 0, offset, id, json));
  }
}
