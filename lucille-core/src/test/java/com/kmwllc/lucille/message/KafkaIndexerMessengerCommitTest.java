package com.kmwllc.lucille.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.KafkaDocument;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

/**
 * Tests how KafkaIndexerMessenger commits destination-topic offsets. Offsets are committed when a batch completes
 * (at-least-once) rather than when a document is polled, so a crash before completion re-delivers the document rather
 * than losing it. Uses a MockConsumer, which tracks committed offsets, to assert the commit behavior directly.
 */
public class KafkaIndexerMessengerCommitTest {

  private static final String TOPIC = KafkaUtils.getDestTopicName("pipeline1");
  private static final TopicPartition PARTITION_0 = new TopicPartition(TOPIC, 0);

  @Test
  public void testOffsetsCommittedOnBatchCompletionNotAtPoll() throws Exception {
    MockConsumer<String, KafkaDocument> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    // The real document consumer is configured with max.poll.records = 1; MockConsumer needs this set explicitly so
    // each poll returns a single record, matching production (and KafkaUtils.validateAtMostOneRecord).
    consumer.setMaxPollRecords(1);
    KafkaIndexerMessenger messenger = new KafkaIndexerMessenger(config(), "pipeline1", consumer);
    consumer.rebalance(Set.of(PARTITION_0));
    consumer.updateBeginningOffsets(Map.of(PARTITION_0, 0L));
    consumer.addRecord(record(0));
    consumer.addRecord(record(1));

    Document first = messenger.pollDocToIndex();
    Document second = messenger.pollDocToIndex();
    // Nothing is committed merely by polling: a crash here must re-deliver both documents, not skip them.
    assertNull(committedOffset(consumer));

    // Completing the first batch commits the offset after its last record (0 -> next offset 1).
    messenger.batchComplete(java.util.List.of(first));
    assertEquals(Long.valueOf(1L), committedOffset(consumer));

    // Completing the second batch advances the commit to 2.
    messenger.batchComplete(java.util.List.of(second));
    assertEquals(Long.valueOf(2L), committedOffset(consumer));
  }

  @Test
  public void testCommitDoesNotSkipAnUncompletedEarlierOffset() throws Exception {
    MockConsumer<String, KafkaDocument> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    consumer.setMaxPollRecords(1);
    KafkaIndexerMessenger messenger = new KafkaIndexerMessenger(config(), "pipeline1", consumer);
    consumer.rebalance(Set.of(PARTITION_0));
    consumer.updateBeginningOffsets(Map.of(PARTITION_0, 0L));
    consumer.addRecord(record(0));
    consumer.addRecord(record(1));
    consumer.addRecord(record(2));

    // Poll three records on one partition. (With indexOverrideField, these could be destined for different indexes and
    // so complete out of offset order — this test completes them in such an order directly.)
    Document offset0 = messenger.pollDocToIndex();
    Document offset1 = messenger.pollDocToIndex();
    Document offset2 = messenger.pollDocToIndex();

    // Complete offset 0, then offset 2, leaving offset 1 still in flight. The committed offset must not pass 1: a crash
    // now must re-deliver offset 1, not skip it. (Committing 3 here would lose offset 1 — the bug this guards against.)
    messenger.batchComplete(java.util.List.of(offset0));
    assertEquals(Long.valueOf(1L), committedOffset(consumer));
    messenger.batchComplete(java.util.List.of(offset2));
    assertEquals("must not commit past the uncompleted offset 1", Long.valueOf(1L), committedOffset(consumer));

    // Completing offset 1 fills the gap, so the commit advances past all three contiguous offsets to 3.
    messenger.batchComplete(java.util.List.of(offset1));
    assertEquals(Long.valueOf(3L), committedOffset(consumer));
  }

  @Test
  public void testRevokedAndReassignedPartitionIsNotRewound() throws Exception {
    MockConsumer<String, KafkaDocument> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    consumer.setMaxPollRecords(1);
    TopicPartition p0 = new TopicPartition(TOPIC, 0);
    TopicPartition p1 = new TopicPartition(TOPIC, 1);
    KafkaIndexerMessenger messenger = new KafkaIndexerMessenger(config(), "pipeline1", consumer);
    consumer.rebalance(Set.of(p0, p1));
    consumer.updateBeginningOffsets(Map.of(p0, 0L, p1, 0L));

    // Poll two records from p0 and one from p1 under this assignment.
    consumer.addRecord(record(p0, 0));
    Document p0First = messenger.pollDocToIndex();
    consumer.addRecord(record(p0, 1));
    Document p0Second = messenger.pollDocToIndex();
    consumer.addRecord(record(p1, 0));
    Document p1First = messenger.pollDocToIndex();

    // p0 is revoked, reassigned to another consumer which indexes further and commits offset 10, then p0 comes back.
    consumer.rebalance(Set.of(p1));
    consumer.commitSync(Map.of(p0, new OffsetAndMetadata(10)));
    consumer.rebalance(Set.of(p0, p1));

    // Our stale batches complete. p0's records were polled under the earlier assignment, so committing their offsets
    // would rewind p0 from 10 back to 2 — this must not happen. p1 is still ours from the same assignment, so it
    // commits normally.
    messenger.batchComplete(java.util.List.of(p0First, p0Second, p1First));
    assertEquals(10L, consumer.committed(Set.of(p0)).get(p0).offset());
    assertEquals(1L, consumer.committed(Set.of(p1)).get(p1).offset());
  }

  @Test
  public void testKeepAlivePollsButDeliversNothingAndKeepsPosition() throws Exception {
    java.util.concurrent.atomic.AtomicInteger pollCount = new java.util.concurrent.atomic.AtomicInteger();
    MockConsumer<String, KafkaDocument> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
      @Override
      public synchronized ConsumerRecords<String, KafkaDocument> poll(java.time.Duration timeout) {
        pollCount.incrementAndGet();
        return super.poll(timeout);
      }
    };
    consumer.setMaxPollRecords(1);
    KafkaIndexerMessenger messenger = new KafkaIndexerMessenger(config(), "pipeline1", consumer);
    consumer.rebalance(Set.of(PARTITION_0));
    consumer.updateBeginningOffsets(Map.of(PARTITION_0, 0L));
    consumer.addRecord(record(0));
    consumer.addRecord(record(1));

    assertEquals("doc-0-0", messenger.pollDocToIndex().getId());
    int pollsAfterFirstDoc = pollCount.get();

    for (int i = 0; i < 5; i++) {
      messenger.keepAlive();
    }
    // keepAlive must actually poll the broker (to stay in the consumer group) — otherwise the consumer would be
    // evicted during a long wait. So the poll count must have advanced.
    assertEquals(pollsAfterFirstDoc + 5, pollCount.get());

    // ...yet it must not deliver a record or move the read position: the next pollDocToIndex still returns the next
    // record in order, and keepAlive committed nothing.
    assertEquals("doc-0-1", messenger.pollDocToIndex().getId());
    assertNull(committedOffset(consumer));
  }

  @Test
  public void testFailedCommitIsRetainedAndRetried() throws Exception {
    java.util.concurrent.atomic.AtomicInteger failuresRemaining = new java.util.concurrent.atomic.AtomicInteger(1);
    MockConsumer<String, KafkaDocument> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
      @Override
      public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (failuresRemaining.getAndDecrement() > 0) {
          throw new org.apache.kafka.clients.consumer.CommitFailedException("simulated commit failure");
        }
        super.commitSync(offsets);
      }
    };
    consumer.setMaxPollRecords(1);
    KafkaIndexerMessenger messenger = new KafkaIndexerMessenger(config(), "pipeline1", consumer);
    consumer.rebalance(Set.of(PARTITION_0));
    consumer.updateBeginningOffsets(Map.of(PARTITION_0, 0L));
    consumer.addRecord(record(0));
    Document first = messenger.pollDocToIndex();
    consumer.addRecord(record(1));
    Document second = messenger.pollDocToIndex();

    // The first batch's commit fails; the offset must be retained, not lost, so nothing is committed yet.
    org.junit.Assert.assertThrows(org.apache.kafka.clients.consumer.CommitFailedException.class,
        () -> messenger.batchComplete(java.util.List.of(first)));
    assertNull(committedOffset(consumer));

    // Completing the next batch retries the retained offset together with the new one; the commit now succeeds at 2.
    messenger.batchComplete(java.util.List.of(second));
    assertEquals(Long.valueOf(2L), committedOffset(consumer));
  }

  @Test
  public void testCommitsOnlyAssignedPartitions() throws Exception {
    MockConsumer<String, KafkaDocument> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    consumer.setMaxPollRecords(1);
    TopicPartition p0 = new TopicPartition(TOPIC, 0);
    TopicPartition p1 = new TopicPartition(TOPIC, 1);
    KafkaIndexerMessenger messenger = new KafkaIndexerMessenger(config(), "pipeline1", consumer);
    consumer.rebalance(Set.of(p0, p1));
    consumer.updateBeginningOffsets(Map.of(p0, 0L, p1, 0L));
    consumer.addRecord(record(p0, 0));
    Document p0First = messenger.pollDocToIndex();
    consumer.addRecord(record(p1, 0));
    Document p1First = messenger.pollDocToIndex();

    // p1 is reassigned to another consumer before our batch completes; p0 stays ours.
    consumer.rebalance(Set.of(p0));

    // The completed batch spans both partitions, but only the still-assigned p0 is committed. Committing p1 could
    // rewind whichever consumer owns it now; its records are simply re-delivered instead.
    messenger.batchComplete(java.util.List.of(p0First, p1First));
    assertEquals(Long.valueOf(1L), committedOffset(consumer, p0));
    assertNull(committedOffset(consumer, p1));
  }

  @Test
  public void testKeepAliveSeeksBackOnNewlyAssignedPartition() throws Exception {
    MockConsumer<String, KafkaDocument> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    consumer.setMaxPollRecords(1);
    KafkaIndexerMessenger messenger = new KafkaIndexerMessenger(config(), "pipeline1", consumer);
    // The partition is assigned during keepAlive's own poll, after the already-assigned partitions were paused, so it
    // is not yet paused and the poll returns its first record. keepAlive must seek back so that record is not consumed.
    consumer.schedulePollTask(() -> {
      consumer.rebalance(Set.of(PARTITION_0));
      consumer.updateBeginningOffsets(Map.of(PARTITION_0, 0L));
      consumer.addRecord(record(0));
    });

    messenger.keepAlive();

    // The read position was rewound to the record keepAlive accidentally pulled, so the next real poll starts there,
    // and nothing was committed. (This mirrors production: keepAlive seeks back so a newly-assigned partition's first
    // record is delivered by pollDocToIndex, not silently consumed by keepAlive.)
    assertEquals(0L, consumer.position(PARTITION_0));
    assertNull(committedOffset(consumer));
  }

  @Test
  public void testKeepAliveWhileJoiningGroupDoesNotAdvancePosition() throws Exception {
    MockConsumer<String, KafkaDocument> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    consumer.setMaxPollRecords(1);
    KafkaIndexerMessenger messenger = new KafkaIndexerMessenger(config(), "pipeline1", consumer);
    // The partition is assigned during the first keepAlive poll, with a record waiting.
    consumer.schedulePollTask(() -> {
      consumer.rebalance(Set.of(PARTITION_0));
      consumer.updateBeginningOffsets(Map.of(PARTITION_0, 0L));
      consumer.addRecord(record(0));
    });

    // Repeated keepAlive while "joining the group" must never commit or advance the read position past the first
    // unread record: whatever a mid-join poll pulls is seeked back so pollDocToIndex still starts at offset 0.
    for (int i = 0; i < 5; i++) {
      messenger.keepAlive();
    }
    assertNull(committedOffset(consumer));
    assertEquals(0L, consumer.position(PARTITION_0));
  }

  @Test
  public void testIdlePollReturnsNullWithoutBlocking() throws Exception {
    MockConsumer<String, KafkaDocument> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    consumer.setMaxPollRecords(1);
    KafkaIndexerMessenger messenger = new KafkaIndexerMessenger(config(), "pipeline1", consumer);
    consumer.rebalance(Set.of(PARTITION_0));
    consumer.updateBeginningOffsets(Map.of(PARTITION_0, 0L));
    consumer.addRecord(record(0));

    assertEquals("doc-0-0", messenger.pollDocToIndex().getId());
    // With nothing more to deliver, each poll returns null promptly rather than blocking a dispatcher waiting on a free
    // slot. (A concurrent indexer polls on the indexer thread between batches, so a long idle block would stall it.)
    for (int i = 0; i < 5; i++) {
      assertNull(messenger.pollDocToIndex());
    }
  }

  private static Config config() {
    return ConfigFactory.parseMap(Map.of(
        "kafka.bootstrapServers", "localhost:9092",
        "kafka.consumerGroupId", "test-group",
        "kafka.maxPollIntervalSecs", 300,
        "kafka.maxRequestSize", 1048576,
        "kafka.events", false));
  }

  // A destination-topic record on partition 0 whose value is a KafkaDocument carrying its own Kafka metadata.
  private static ConsumerRecord<String, KafkaDocument> record(long offset) {
    return record(PARTITION_0, offset);
  }

  // A destination-topic record on the given partition whose value is a KafkaDocument carrying its own Kafka metadata.
  private static ConsumerRecord<String, KafkaDocument> record(TopicPartition partition, long offset) {
    String id = "doc-" + partition.partition() + "-" + offset;
    try {
      KafkaDocument doc = new KafkaDocument(
          new ConsumerRecord<>(partition.topic(), partition.partition(), offset, id, Document.create(id, "run1").toString()));
      return new ConsumerRecord<>(partition.topic(), partition.partition(), offset, id, doc);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // The committed offset for partition 0, or null if nothing has been committed.
  private static Long committedOffset(MockConsumer<String, KafkaDocument> consumer) {
    return committedOffset(consumer, PARTITION_0);
  }

  // The committed offset for the given partition, or null if nothing has been committed.
  private static Long committedOffset(MockConsumer<String, KafkaDocument> consumer, TopicPartition partition) {
    OffsetAndMetadata committed = consumer.committed(Set.of(partition)).get(partition);
    return committed == null ? null : committed.offset();
  }
}
