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
 * path, so it is exercised here directly: records are "polled" and "completed" by calling the tracker's own methods,
 * and committed offsets are read back from a {@link MockConsumer}.
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
    poll(tracker, P0, 0);
    poll(tracker, P0, 1);
    poll(tracker, P0, 2);
    // Nothing is committed by polling alone.
    assertNull(committed(consumer, P0));

    // Completing offset 0 commits the frontier to 1 (the next offset to read).
    tracker.batchCompleted(docs(P0, 0));
    assertEquals(Long.valueOf(1), committed(consumer, P0));
    // Completing 1 then 2 advances the frontier contiguously to 3.
    tracker.batchCompleted(docs(P0, 1));
    assertEquals(Long.valueOf(2), committed(consumer, P0));
    tracker.batchCompleted(docs(P0, 2));
    assertEquals(Long.valueOf(3), committed(consumer, P0));
  }

  @Test
  public void testFrontierDoesNotPassAnUncompletedEarlierOffset() {
    MockConsumer<String, KafkaDocument> consumer = assignedConsumer(Set.of(P0));
    OffsetCommitTracker tracker = new OffsetCommitTracker(consumer);
    poll(tracker, P0, 0);
    poll(tracker, P0, 1);
    poll(tracker, P0, 2);

    // Complete 0, then 2 out of order, leaving 1 in flight. The frontier must stop at 1: a crash here must re-deliver
    // offset 1 rather than skip it. Committing 3 would lose offset 1 -- the bug the frontier guards against.
    tracker.batchCompleted(docs(P0, 0));
    assertEquals(Long.valueOf(1), committed(consumer, P0));
    tracker.batchCompleted(docs(P0, 2));
    assertEquals("must not pass the uncompleted offset 1", Long.valueOf(1), committed(consumer, P0));

    // Filling the gap at 1 releases the whole contiguous run up to 3.
    tracker.batchCompleted(docs(P0, 1));
    assertEquals(Long.valueOf(3), committed(consumer, P0));
  }

  @Test
  public void testFrontierIsIndependentPerPartition() {
    MockConsumer<String, KafkaDocument> consumer = assignedConsumer(Set.of(P0, P1));
    OffsetCommitTracker tracker = new OffsetCommitTracker(consumer);
    poll(tracker, P0, 0);
    poll(tracker, P0, 1);
    poll(tracker, P1, 0);

    // A batch spanning both partitions advances each partition's own frontier independently.
    tracker.batchCompleted(docs(P0, 0, P1, 0));
    assertEquals(Long.valueOf(1), committed(consumer, P0));
    assertEquals(Long.valueOf(1), committed(consumer, P1));
    // P0 still has offset 1 outstanding; completing it advances only P0.
    tracker.batchCompleted(docs(P0, 1));
    assertEquals(Long.valueOf(2), committed(consumer, P0));
    assertEquals(Long.valueOf(1), committed(consumer, P1));
  }

  @Test
  public void testCommitsOnlyPartitionsStillAssigned() {
    MockConsumer<String, KafkaDocument> consumer = assignedConsumer(Set.of(P0, P1));
    OffsetCommitTracker tracker = new OffsetCommitTracker(consumer);
    poll(tracker, P0, 0);
    poll(tracker, P1, 0);

    // P1 is reassigned away from this consumer before its batch completes.
    consumer.rebalance(Set.of(P0));

    // The completed batch holds records from both partitions, but only the still-assigned P0 is committed; committing
    // P1 could rewind whichever consumer owns it now.
    tracker.batchCompleted(docs(P0, 0, P1, 0));
    assertEquals(Long.valueOf(1), committed(consumer, P0));
    assertNull(committed(consumer, P1));
  }

  @Test
  public void testRevokedAndReassignedPartitionIsNotRewound() {
    MockConsumer<String, KafkaDocument> consumer = assignedConsumer(Set.of(P0, P1));
    OffsetCommitTracker tracker = new OffsetCommitTracker(consumer);
    // Poll two records on P0 and one on P1 under this assignment.
    poll(tracker, P0, 0);
    poll(tracker, P0, 1);
    poll(tracker, P1, 0);

    // P0 is revoked (notifying the tracker's rebalance listener), handed to another consumer that commits offset 10,
    // then comes back to us.
    tracker.rebalanceListener().onPartitionsRevoked(Set.of(P0));
    consumer.commitSync(Map.of(P0, new OffsetAndMetadata(10)));
    tracker.rebalanceListener().onPartitionsAssigned(Set.of(P0));

    // The stale P0 batches complete. Their offsets were polled under the earlier assignment, so committing them would
    // rewind P0 from 10 back to 2 -- that must not happen. P1 stayed ours, so it commits normally.
    tracker.batchCompleted(List.of(kafkaDoc(P0, 0), kafkaDoc(P0, 1), kafkaDoc(P1, 0)));
    assertEquals(Long.valueOf(10), committed(consumer, P0));
    assertEquals(Long.valueOf(1), committed(consumer, P1));
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
    poll(tracker, P0, 0);
    poll(tracker, P0, 1);

    // The first commit fails; the offset must be retained, not dropped, so nothing is committed yet.
    assertThrows(CommitFailedException.class, () -> tracker.batchCompleted(docs(P0, 0)));
    assertNull(committed(consumer, P0));

    // Completing the next batch retries the retained offset together with the new one; the commit now reaches 2.
    tracker.batchCompleted(docs(P0, 1));
    assertEquals(Long.valueOf(2), committed(consumer, P0));
  }

  @Test
  public void testCommitOnCloseFlushesPendingFrontier() {
    MockConsumer<String, KafkaDocument> consumer = assignedConsumer(Set.of(P0));
    OffsetCommitTracker tracker = new OffsetCommitTracker(consumer);
    poll(tracker, P0, 0);
    tracker.batchCompleted(docs(P0, 0));
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

  // Tells the tracker a record at the given offset was polled on the given partition.
  private static void poll(OffsetCommitTracker tracker, TopicPartition partition, long offset) {
    tracker.recordPolled(partition, offset + 1);
  }

  // A completed batch of one record at the given partition/offset.
  private static List<Document> docs(TopicPartition partition, long offset) {
    return List.of(kafkaDoc(partition, offset));
  }

  // A completed batch of two records, one on each given partition/offset.
  private static List<Document> docs(TopicPartition pa, long oa, TopicPartition pb, long ob) {
    return List.of(kafkaDoc(pa, oa), kafkaDoc(pb, ob));
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
