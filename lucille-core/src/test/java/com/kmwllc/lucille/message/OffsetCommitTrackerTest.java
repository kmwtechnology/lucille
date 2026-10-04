package com.kmwllc.lucille.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.KafkaDocument;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

/**
 * Unit tests for {@link OffsetCommitTracker} in isolation from {@link KafkaIndexerMessenger}. The tracker holds the
 * frontier logic that keeps commit-on-completion at-least-once, which is the riskiest part of the concurrent Kafka
 * path, so it is exercised here directly: records are "polled" (which stamps the document with the generation the
 * tracker returns, as the real messenger does) and "completed" by calling the tracker's own methods, and committed
 * offsets are read back from a {@link MockConsumer}.
 */
public class OffsetCommitTrackerTest {

  private static final String TOPIC = "dest";
  private static final TopicPartition P0 = new TopicPartition(TOPIC, 0);
  private static final TopicPartition P1 = new TopicPartition(TOPIC, 1);

  @Test
  public void testFrontierAdvancesAcrossContiguousCompletedOffsets() {
    MockConsumer<String, KafkaDocument> consumer = assignedConsumer(Set.of(P0));
    OffsetCommitTracker tracker = new OffsetCommitTracker(consumer);

    // Poll three records, 0..2, in order.
    KafkaDocument d0 = poll(tracker, P0, 0);
    KafkaDocument d1 = poll(tracker, P0, 1);
    KafkaDocument d2 = poll(tracker, P0, 2);
    // Nothing is committed by polling alone.
    assertNull(committed(consumer, P0));

    // Completing offset 0 commits the frontier to 1 (the next offset to read).
    tracker.batchCompleted(List.of(d0));
    assertEquals(Long.valueOf(1), committed(consumer, P0));
    // Completing 1 then 2 advances the frontier contiguously to 3.
    tracker.batchCompleted(List.of(d1));
    assertEquals(Long.valueOf(2), committed(consumer, P0));
    tracker.batchCompleted(List.of(d2));
    assertEquals(Long.valueOf(3), committed(consumer, P0));
  }

  @Test
  public void testFrontierDoesNotPassAnUncompletedEarlierOffset() {
    MockConsumer<String, KafkaDocument> consumer = assignedConsumer(Set.of(P0));
    OffsetCommitTracker tracker = new OffsetCommitTracker(consumer);
    KafkaDocument d0 = poll(tracker, P0, 0);
    KafkaDocument d1 = poll(tracker, P0, 1);
    KafkaDocument d2 = poll(tracker, P0, 2);

    // Complete 0, then 2 out of order, leaving 1 in flight. The frontier must stop at 1: a crash here must re-deliver
    // offset 1 rather than skip it. Committing 3 would lose offset 1 -- the bug the frontier guards against.
    tracker.batchCompleted(List.of(d0));
    assertEquals(Long.valueOf(1), committed(consumer, P0));
    tracker.batchCompleted(List.of(d2));
    assertEquals("must not pass the uncompleted offset 1", Long.valueOf(1), committed(consumer, P0));

    // Filling the gap at 1 releases the whole contiguous run up to 3.
    tracker.batchCompleted(List.of(d1));
    assertEquals(Long.valueOf(3), committed(consumer, P0));
  }

  @Test
  public void testFrontierAdvancesOverGapsInThePolledSequence() {
    // The polled offsets are not contiguous (e.g. a compacted or transactional topic skips 1 and 3). The frontier must
    // advance across the offsets actually polled rather than wait for the missing ones, which never arrive.
    MockConsumer<String, KafkaDocument> consumer = assignedConsumer(Set.of(P0));
    OffsetCommitTracker tracker = new OffsetCommitTracker(consumer);
    KafkaDocument d0 = poll(tracker, P0, 0);
    KafkaDocument d2 = poll(tracker, P0, 2);
    KafkaDocument d4 = poll(tracker, P0, 4);

    tracker.batchCompleted(List.of(d0));
    assertEquals(Long.valueOf(1), committed(consumer, P0));
    // Completing offset 2 (offset 1 was never polled) advances to 3, not stuck at 1.
    tracker.batchCompleted(List.of(d2));
    assertEquals("frontier must skip the never-polled offset 1", Long.valueOf(3), committed(consumer, P0));
    tracker.batchCompleted(List.of(d4));
    assertEquals(Long.valueOf(5), committed(consumer, P0));
  }

  @Test
  public void testFrontierIsIndependentPerPartition() {
    MockConsumer<String, KafkaDocument> consumer = assignedConsumer(Set.of(P0, P1));
    OffsetCommitTracker tracker = new OffsetCommitTracker(consumer);
    KafkaDocument p0o0 = poll(tracker, P0, 0);
    KafkaDocument p0o1 = poll(tracker, P0, 1);
    KafkaDocument p1o0 = poll(tracker, P1, 0);

    // A batch spanning both partitions advances each partition's own frontier independently.
    tracker.batchCompleted(List.of(p0o0, p1o0));
    assertEquals(Long.valueOf(1), committed(consumer, P0));
    assertEquals(Long.valueOf(1), committed(consumer, P1));
    // P0 still has offset 1 outstanding; completing it advances only P0.
    tracker.batchCompleted(List.of(p0o1));
    assertEquals(Long.valueOf(2), committed(consumer, P0));
    assertEquals(Long.valueOf(1), committed(consumer, P1));
  }

  @Test
  public void testCommitsOnlyPartitionsStillAssigned() {
    MockConsumer<String, KafkaDocument> consumer = assignedConsumer(Set.of(P0, P1));
    OffsetCommitTracker tracker = new OffsetCommitTracker(consumer);
    KafkaDocument p0o0 = poll(tracker, P0, 0);
    KafkaDocument p1o0 = poll(tracker, P1, 0);

    // P1 is reassigned away from this consumer before its batch completes.
    consumer.rebalance(Set.of(P0));

    // The completed batch holds records from both partitions, but only the still-assigned P0 is committed; committing
    // P1 could rewind whichever consumer owns it now.
    tracker.batchCompleted(List.of(p0o0, p1o0));
    assertEquals(Long.valueOf(1), committed(consumer, P0));
    assertNull(committed(consumer, P1));
  }

  @Test
  public void testRevokedAndReassignedPartitionIsNotRewound() {
    MockConsumer<String, KafkaDocument> consumer = assignedConsumer(Set.of(P0, P1));
    OffsetCommitTracker tracker = new OffsetCommitTracker(consumer);
    // Poll two records on P0 and one on P1 under this assignment.
    KafkaDocument p0o0 = poll(tracker, P0, 0);
    KafkaDocument p0o1 = poll(tracker, P0, 1);
    KafkaDocument p1o0 = poll(tracker, P1, 0);

    // P0 is revoked (notifying the tracker's rebalance listener), handed to another consumer that commits offset 10,
    // then comes back to us.
    tracker.rebalanceListener().onPartitionsRevoked(Set.of(P0));
    consumer.commitSync(Map.of(P0, new OffsetAndMetadata(10)));
    tracker.rebalanceListener().onPartitionsAssigned(Set.of(P0));

    // The stale P0 batches complete. Their offsets were polled under the earlier generation, so committing them would
    // rewind P0 from 10 back to 2 -- that must not happen. P1 stayed ours, so it commits normally.
    tracker.batchCompleted(List.of(p0o0, p0o1, p1o0));
    assertEquals(Long.valueOf(10), committed(consumer, P0));
    assertEquals(Long.valueOf(1), committed(consumer, P1));
  }

  @Test
  public void testEagerRebalanceToSameConsumerStillCommitsLaterProgress() {
    // kafka-clients defaults to eager rebalancing: a partition is typically revoked and handed back to the SAME
    // consumer. Records polled before the revoke are re-polled from the committed offset under a new generation. The
    // pre-revoke completions must be ignored (older generation) while the re-polled ones commit and the frontier keeps
    // advancing -- the earlier watermark design stalled here and never committed past 0.
    MockConsumer<String, KafkaDocument> consumer = assignedConsumer(Set.of(P0));
    OffsetCommitTracker tracker = new OffsetCommitTracker(consumer);

    // Poll 0..9 under generation 0.
    List<KafkaDocument> preRevoke = new java.util.ArrayList<>();
    for (long offset = 0; offset < 10; offset++) {
      preRevoke.add(poll(tracker, P0, offset));
    }

    // Eager revoke and immediate reassignment to the same consumer: generation bumps, outstanding polled state clears.
    tracker.rebalanceListener().onPartitionsRevoked(Set.of(P0));
    tracker.rebalanceListener().onPartitionsAssigned(Set.of(P0));

    // The pre-revoke batches now complete. They are from the old generation and must be ignored, not stall the frontier.
    for (KafkaDocument doc : preRevoke) {
      tracker.batchCompleted(List.of(doc));
    }
    assertNull("stale pre-revoke completions must not commit", committed(consumer, P0));

    // Re-poll from the committed offset (0) under the new generation and index 0..999; the frontier must advance.
    for (long offset = 0; offset < 1000; offset++) {
      tracker.batchCompleted(List.of(poll(tracker, P0, offset)));
    }
    assertEquals("frontier must advance under the new generation", Long.valueOf(1000), committed(consumer, P0));
  }

  @Test
  public void testFailedCommitIsRetainedAndRetried() {
    int[] failuresRemaining = {1};
    MockConsumer<String, KafkaDocument> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
      @Override
      public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (failuresRemaining[0]-- > 0) {
          throw new CommitFailedException("simulated commit failure");
        }
        super.commitSync(offsets);
      }
    };
    consumer.subscribe(List.of(TOPIC));
    consumer.rebalance(Set.of(P0));
    OffsetCommitTracker tracker = new OffsetCommitTracker(consumer);
    KafkaDocument d0 = poll(tracker, P0, 0);
    KafkaDocument d1 = poll(tracker, P0, 1);

    // The first commit fails; the offset must be retained, not dropped, so nothing is committed yet.
    assertThrows(CommitFailedException.class, () -> tracker.batchCompleted(List.of(d0)));
    assertNull(committed(consumer, P0));

    // Completing the next batch retries the retained offset together with the new one; the commit now reaches 2.
    tracker.batchCompleted(List.of(d1));
    assertEquals(Long.valueOf(2), committed(consumer, P0));
  }

  @Test
  public void testCommitOnCloseFlushesPendingFrontier() {
    MockConsumer<String, KafkaDocument> consumer = assignedConsumer(Set.of(P0));
    OffsetCommitTracker tracker = new OffsetCommitTracker(consumer);
    tracker.batchCompleted(List.of(poll(tracker, P0, 0)));
    assertEquals(Long.valueOf(1), committed(consumer, P0));

    // commitOnClose with nothing further pending is a no-op and must not throw or change the committed offset.
    tracker.commitOnClose();
    assertEquals(Long.valueOf(1), committed(consumer, P0));
  }

  // --- helpers ---

  // A MockConsumer subscribed to the topic (as the messenger would) and already owning the given partitions.
  private static MockConsumer<String, KafkaDocument> assignedConsumer(Set<TopicPartition> partitions) {
    MockConsumer<String, KafkaDocument> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    consumer.subscribe(List.of(TOPIC));
    consumer.rebalance(partitions);
    return consumer;
  }

  // Records the poll with the tracker and returns the document stamped with the generation the tracker reports -- the
  // same handshake the real KafkaIndexerMessenger performs in pollDocToIndex.
  private static KafkaDocument poll(OffsetCommitTracker tracker, TopicPartition partition, long offset) {
    long generation = tracker.recordPolled(partition, offset);
    KafkaDocument doc = kafkaDoc(partition, offset);
    doc.setDeliveryGeneration(generation);
    return doc;
  }

  private static KafkaDocument kafkaDoc(TopicPartition partition, long offset) {
    String id = "doc-" + partition.partition() + "-" + offset;
    try {
      KafkaDocument doc = new KafkaDocument(new ConsumerRecord<>(
          partition.topic(), partition.partition(), offset, id, Document.create(id, "run1").toString()));
      doc.setKafkaMetadata(new ConsumerRecord<>(partition.topic(), partition.partition(), offset, id, doc));
      return doc;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // The committed offset for a partition, or null if nothing is committed.
  private static Long committed(MockConsumer<String, KafkaDocument> consumer, TopicPartition partition) {
    OffsetAndMetadata committed = consumer.committed(Set.of(partition)).get(partition);
    return committed == null ? null : committed.offset();
  }
}
