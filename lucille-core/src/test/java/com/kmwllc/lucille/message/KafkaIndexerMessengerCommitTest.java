package com.kmwllc.lucille.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.KafkaDocument;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests how KafkaIndexerMessenger commits destination-topic offsets, driving a MockConsumer: at poll when
 * indexer.maxConcurrentBatches is 1, and in batchComplete when it is greater.
 */
public class KafkaIndexerMessengerCommitTest {

  private static final String TOPIC = KafkaUtils.getDestTopicName("mock");
  private static final TopicPartition P0 = new TopicPartition(TOPIC, 0);
  private static final TopicPartition P1 = new TopicPartition(TOPIC, 1);

  private RecordingConsumer consumer;

  @Before
  public void setUp() {
    consumer = new RecordingConsumer();
    consumer.setMaxPollRecords(1);
  }

  @Test
  public void testSingleBatchCommitsAtPoll() throws Exception {
    KafkaIndexerMessenger messenger = messenger(1, P0);
    Document first = poll(messenger, P0, 0);
    // committed as soon as it is polled, before the batch completes
    assertEquals(Long.valueOf(1), committed(P0));
    messenger.batchComplete(List.of(first));
    poll(messenger, P0, 1);
    assertEquals(Long.valueOf(2), committed(P0));
    messenger.close();
  }

  @Test
  public void testConcurrentCommitsOnBatchCompleteNotAtPoll() throws Exception {
    KafkaIndexerMessenger messenger = messenger(3, P0, P1);
    List<Document> docs = List.of(poll(messenger, P0, 0), poll(messenger, P1, 0), poll(messenger, P0, 1),
        poll(messenger, P0, 2));
    assertNull("nothing is committed at poll", committed(P0));
    assertNull(committed(P1));

    messenger.batchComplete(docs.subList(0, 3));
    // the offset after each partition's last record in the batch
    assertEquals(Long.valueOf(2), committed(P0));
    assertEquals(Long.valueOf(1), committed(P1));

    messenger.batchComplete(docs.subList(3, 4));
    assertEquals(Long.valueOf(3), committed(P0));
    messenger.close();
  }

  @Test
  public void testConcurrentDropsUnassignedPartitions() throws Exception {
    KafkaIndexerMessenger messenger = messenger(3, P0, P1);
    List<Document> docs = List.of(poll(messenger, P0, 0), poll(messenger, P1, 0));

    // P1 moves to another consumer while the batch is in flight
    consumer.rebalance(List.of(P0));
    messenger.batchComplete(docs);
    assertEquals(Long.valueOf(1), committed(P0));
    assertNull(committed(P1));

    // P1's offset was dropped, not kept for a later commit
    consumer.rebalance(List.of(P0, P1));
    messenger.batchComplete(List.of(poll(messenger, P0, 1)));
    assertEquals(Long.valueOf(2), committed(P0));
    assertNull(committed(P1));
    messenger.close();
  }

  @Test
  public void testFailedCommitIsRetriedWithNextBatch() throws Exception {
    KafkaIndexerMessenger messenger = messenger(3, P0, P1);
    Document first = poll(messenger, P1, 0);
    Document second = poll(messenger, P0, 0);

    consumer.failures.set(1);
    messenger.batchComplete(List.of(first));
    assertNull(committed(P1));

    messenger.batchComplete(List.of(second));
    assertEquals(Long.valueOf(1), committed(P0));
    assertEquals(Long.valueOf(1), committed(P1));
    messenger.close();
  }

  @Test
  public void testCloseCommitsPending() throws Exception {
    KafkaIndexerMessenger messenger = messenger(3, P0);
    consumer.failures.set(1);
    messenger.batchComplete(List.of(poll(messenger, P0, 0)));
    assertTrue(consumer.commits.isEmpty());
    messenger.close();
    assertEquals(List.of(Map.of(P0, new OffsetAndMetadata(1))), consumer.commits);
  }

  @Test
  public void testConcurrentRejectsAutoCommit() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> new KafkaIndexerMessenger(config(3, Map.of("kafka.consumer.enable.auto.commit", true)), "mock", consumer));
    assertTrue(e.getMessage(), e.getMessage().contains("enable.auto.commit"));
    assertTrue(consumer.closed());
  }

  @Test
  public void testSingleBatchAllowsAutoCommit() throws Exception {
    new KafkaIndexerMessenger(config(1, Map.of("kafka.consumer.enable.auto.commit", true)), "mock", consumer).close();
  }

  // --- helpers ---

  private static Config config(int maxConcurrentBatches, Map<String, Object> extra) {
    Map<String, Object> settings = new HashMap<>(Map.of(
        "kafka.bootstrapServers", "localhost:9092",
        "kafka.consumerGroupId", "mock",
        "kafka.maxPollIntervalSecs", 300,
        "kafka.events", false,
        "indexer.maxConcurrentBatches", maxConcurrentBatches));
    settings.putAll(extra);
    return ConfigFactory.parseMap(settings);
  }

  private KafkaIndexerMessenger messenger(int maxConcurrentBatches, TopicPartition... partitions) {
    KafkaIndexerMessenger messenger = new KafkaIndexerMessenger(config(maxConcurrentBatches, Map.of()), "mock", consumer);
    consumer.rebalance(List.of(partitions));
    for (TopicPartition partition : partitions) {
      consumer.updateBeginningOffsets(Map.of(partition, 0L));
    }
    return messenger;
  }

  // adds a record at the given offset and polls it
  private KafkaDocument poll(KafkaIndexerMessenger messenger, TopicPartition partition, long offset) throws Exception {
    consumer.addRecord(record(partition, offset));
    KafkaDocument doc = (KafkaDocument) messenger.pollDocToIndex();
    assertEquals(offset, doc.getOffset());
    return doc;
  }

  private Long committed(TopicPartition partition) {
    OffsetAndMetadata offset = consumer.committed(Collections.singleton(partition)).get(partition);
    return offset == null ? null : offset.offset();
  }

  private static ConsumerRecord<String, KafkaDocument> record(TopicPartition partition, long offset) {
    String id = "doc-" + partition.partition() + "-" + offset;
    try {
      KafkaDocument doc = new KafkaDocument(new ConsumerRecord<>(partition.topic(), partition.partition(), offset, id,
          Document.create(id, "run").toString()));
      return new ConsumerRecord<>(partition.topic(), partition.partition(), offset, id, doc);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Records explicit-offset commits, and can fail the next few.
  private static class RecordingConsumer extends MockConsumer<String, KafkaDocument> {

    final List<Map<TopicPartition, OffsetAndMetadata>> commits = new ArrayList<>();
    final AtomicInteger failures = new AtomicInteger();

    RecordingConsumer() {
      super("earliest");
    }

    @Override
    public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
      if (failures.getAndDecrement() > 0) {
        throw new CommitFailedException("simulated");
      }
      commits.add(Map.copyOf(offsets));
      super.commitSync(offsets);
    }
  }
}
