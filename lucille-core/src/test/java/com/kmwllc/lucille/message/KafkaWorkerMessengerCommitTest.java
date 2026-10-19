package com.kmwllc.lucille.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.KafkaDocument;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the standalone worker's batched source-offset commits: an offset is committed only after the dest-topic
 * sends it covers are acknowledged, and only once the commit batch is full, has timed out, or partitions are revoked
 * or the messenger is closed.
 */
public class KafkaWorkerMessengerCommitTest {

  private static final String PIPELINE = "pipeline1";
  private static final String SOURCE_TOPIC = "pipeline1_source";
  private static final TopicPartition TP0 = new TopicPartition(SOURCE_TOPIC, 0);
  private static final TopicPartition TP1 = new TopicPartition(SOURCE_TOPIC, 1);

  private MockConsumer<String, KafkaDocument> consumer;
  private MockProducer<String, Document> docProducer;
  private MockProducer<String, String> eventProducer;
  private long nextOffset;
  private Map<TopicPartition, Long> commits;
  private int eventsSentAtLastCommit;

  @Before
  public void setUp() {
    commits = new ConcurrentHashMap<>();
    // records every commit, so offsets stay inspectable after a partition is revoked or the consumer is closed
    consumer = new MockConsumer<>("earliest") {
      @Override
      public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        super.commitSync(offsets);
        offsets.forEach((tp, offset) -> commits.put(tp, offset.offset()));
        eventsSentAtLastCommit = eventProducer.history().size();
      }
    };
    // autoComplete=false: a send stays unacknowledged until flush() or completeNext()/errorNext()
    docProducer = new MockProducer<>(false, null, new StringSerializer(),
        (topic, doc) -> doc.toString().getBytes(StandardCharsets.UTF_8));
    eventProducer = new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
    nextOffset = 0;
  }

  private KafkaWorkerMessenger messenger(int commitBatchSize, int commitBatchTimeout) {
    Config config = ConfigFactory.parseString(
        "worker.commitBatchSize: " + commitBatchSize + "\nworker.commitBatchTimeout: " + commitBatchTimeout);
    KafkaWorkerMessenger messenger = new KafkaWorkerMessenger(config, PIPELINE, consumer, docProducer, eventProducer);
    consumer.rebalance(List.of(TP0, TP1));
    consumer.updateBeginningOffsets(Map.of(TP0, 0L, TP1, 0L));
    return messenger;
  }

  /** Polls one document from partition 0, sends it for indexing, and asks the messenger to commit. */
  private Document processOne(KafkaWorkerMessenger messenger) throws Exception {
    return processOne(messenger, TP0);
  }

  private Document processOne(KafkaWorkerMessenger messenger, TopicPartition tp) throws Exception {
    long offset = nextOffset++;
    KafkaDocument doc = kafkaDoc("doc" + offset);
    consumer.addRecord(new ConsumerRecord<>(tp.topic(), tp.partition(), offset, doc.getId(), doc));
    Document polled = messenger.pollDocToProcess();
    assertEquals(doc.getId(), polled.getId());
    messenger.sendForIndexing(polled);
    messenger.commitPendingDocOffsets();
    return polled;
  }

  private static KafkaDocument kafkaDoc(String id) throws Exception {
    return new KafkaDocument((ObjectNode) new ObjectMapper().readTree(Document.create(id, "run1").toString()));
  }

  private Long committed(TopicPartition tp) {
    return commits.get(tp);
  }

  @Test(timeout = 30000)
  public void testCommitsOnlyWhenBatchIsFull() throws Exception {
    KafkaWorkerMessenger messenger = messenger(3, 60000);

    processOne(messenger);
    processOne(messenger);
    assertNull("no commit before the batch is full", committed(TP0));
    assertEquals("sends are not waited on per document", 2, docProducer.history().size());

    processOne(messenger);
    assertEquals(Long.valueOf(3), committed(TP0));
    assertTrue("the batch's sends were flushed before the commit", docProducer.flushed());

    processOne(messenger);
    assertEquals("a new batch starts after the commit", Long.valueOf(3), committed(TP0));
  }

  @Test(timeout = 30000)
  public void testCommitBatchSizeOneCommitsEveryDocument() throws Exception {
    KafkaWorkerMessenger messenger = messenger(1, 60000);
    processOne(messenger);
    assertEquals(Long.valueOf(1), committed(TP0));
    processOne(messenger);
    assertEquals(Long.valueOf(2), committed(TP0));
  }

  @Test(timeout = 30000)
  public void testIdleWorkerCommitsAfterTimeout() throws Exception {
    KafkaWorkerMessenger messenger = messenger(100, 50);
    processOne(messenger);
    assertNull(committed(TP0));

    Thread.sleep(100);
    // an empty poll followed by the Worker's commit call, as on an idle Worker
    assertNull(messenger.pollDocToProcess());
    messenger.commitPendingDocOffsets();
    assertEquals(Long.valueOf(1), committed(TP0));
  }

  @Test(timeout = 30000)
  public void testNothingCommittedWhileSendIsUnacknowledged() throws Exception {
    KafkaWorkerMessenger messenger = messenger(1, 60000);
    long offset = nextOffset++;
    KafkaDocument doc = kafkaDoc("doc" + offset);
    consumer.addRecord(new ConsumerRecord<>(SOURCE_TOPIC, 0, offset, doc.getId(), doc));
    messenger.sendForIndexing(messenger.pollDocToProcess());

    // the broker never acknowledges, so flush() fails and nothing may be committed
    docProducer.flushException = new RuntimeException("broker unavailable");
    assertThrows(RuntimeException.class, messenger::commitPendingDocOffsets);
    assertNull(committed(TP0));

    // once the send is acknowledged, the retried commit goes through
    docProducer.flushException = null;
    messenger.commitPendingDocOffsets();
    assertEquals(Long.valueOf(1), committed(TP0));
  }

  @Test(timeout = 30000)
  public void testRejectedSendIsReportedAsFailBeforeCommit() throws Exception {
    KafkaWorkerMessenger messenger = messenger(2, 60000);
    Document first = processOne(messenger);
    docProducer.errorNext(new RuntimeException("record too large"));

    processOne(messenger);
    assertEquals(Long.valueOf(2), committed(TP0));

    List<Event> events = eventProducer.history().stream().map(r -> {
      try {
        return Event.fromJsonString(r.value());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }).toList();
    assertEquals(1, events.size());
    assertEquals("the FAIL event was sent before the commit", 1, eventsSentAtLastCommit);
    assertEquals(Event.Type.FAIL, events.get(0).getType());
    assertEquals(first.getId(), events.get(0).getDocumentId());
  }

  @Test(timeout = 30000)
  public void testRevocationCommitsPendingOffsets() throws Exception {
    KafkaWorkerMessenger messenger = messenger(100, 60000);
    processOne(messenger, TP0);
    processOne(messenger, TP1);
    assertNull(committed(TP0));

    consumer.rebalance(List.of(TP1));
    assertEquals(Long.valueOf(1), committed(TP0));
    assertEquals(Long.valueOf(2), committed(TP1));
    assertTrue(docProducer.flushed());
  }

  @Test(timeout = 30000)
  public void testCloseCommitsPendingOffsets() throws Exception {
    KafkaWorkerMessenger messenger = messenger(100, 60000);
    processOne(messenger);
    processOne(messenger);
    assertNull(committed(TP0));
    assertFalse(docProducer.flushed());

    messenger.close();
    assertEquals(Long.valueOf(2), committed(TP0));
  }

  @Test(timeout = 30000)
  public void testRejectsNonPositiveSettings() {
    assertThrows(IllegalArgumentException.class, () -> messenger(0, 100));
    assertThrows(IllegalArgumentException.class, () -> messenger(100, 0));
  }
}
