package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.KafkaDocument;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks and commits destination-topic offsets for {@link KafkaIndexerMessenger}, committing each batch's offsets when
 * the batch completes (at-least-once) rather than when its documents were polled. This is what makes a crash re-deliver
 * documents rather than lose them.
 *
 * <p> Assumes the destination topic has no offset gaps — every offset the consumer could read is delivered as a record.
 * This holds for Lucille's destination topic: its producer is non-transactional (see {@code KafkaUtils.createProducerProps},
 * which sets {@code enable.idempotence} but no {@code transactional.id}), so there are no transaction-marker offsets,
 * and the topic is not compacted. The frontier advances only across <i>contiguous</i> completed offsets, so a gap it
 * never sees as a record (a transaction marker, or a record removed by compaction) would stall it: offsets past the gap
 * stay uncommitted and their records are re-delivered. That is at-least-once, not loss — but if this tracker is ever
 * pointed at a transactional or compacted topic, the frontier would need to advance over such gaps rather than wait for
 * them to fill.
 *
 * <p> Correctness:
 * <ul>
 *   <li><b>No lost document.</b> Per partition, the committed offset is a <i>frontier</i>: it advances only across a
 *   contiguous run of completed offsets, so every record below it has been indexed. It never passes an uncompleted
 *   earlier record — which can arise from out-of-order completion generally, and specifically from MultiBatch, where
 *   per-index sub-batches let a higher-offset record complete while a lower-offset one is still unflushed. A crash
 *   re-reads from the frontier and loses nothing. (This holds independently of the order in which batches complete.)</li>
 *   <li><b>No rewind of another consumer.</b> Only partitions still assigned to this consumer are committed, and offsets
 *   of records polled under an earlier assignment of a partition (revoked and possibly reassigned since) are dropped,
 *   because another consumer may have committed further. Dropping an offset only re-delivers its records.</li>
 *   <li><b>No lost commit on failure.</b> A failed commit is retained and retried on the next completed batch, on
 *   revocation, and on close.</li>
 * </ul>
 *
 * <p> All methods, and the {@link ConsumerRebalanceListener} this exposes, run on the indexer thread (the listener is
 * invoked by the consumer from within poll() or close()), so the state here needs no synchronization.
 */
class OffsetCommitTracker {

  private static final Logger log = LoggerFactory.getLogger(OffsetCommitTracker.class);

  private final Consumer<?, ?> consumer;

  // Offset convention: a "...Through" offset is an exclusive upper bound — the offset *after* the last record it refers
  // to, i.e. the next offset to read — matching Kafka's committed-offset semantics. lowestPolled is the exception noted
  // below: it is an inclusive offset, because "the lowest offset polled" is naturally a record's own offset.
  //
  // Per assigned partition, the lowest offset polled but not yet absorbed into the commit frontier (inclusive). Used to
  // seed the frontier (the consumer resumes contiguously from its committed position, so the lowest polled offset is
  // that position) and to decide staleness after a rebalance.
  private final Map<TopicPartition, Long> lowestPolled = new HashMap<>();
  // Per assigned partition, the offset after the last record polled in the current assignment.
  private final Map<TopicPartition, Long> polledThrough = new HashMap<>();
  // Per partition, the commit frontier: the next offset to commit, below which every record has completed. Advances
  // only across a contiguous run of completed offsets, so it never passes an uncompleted earlier record (as can happen
  // with MultiBatch per-index flushing, or any out-of-order completion). Named a frontier, not "...Through", because it
  // advances before anything is committed — a commit may still fail and be retried.
  private final Map<TopicPartition, Long> commitFrontier = new HashMap<>();
  // Per partition, completed offsets at or above the frontier, waiting for the gap below them to fill.
  private final Map<TopicPartition, java.util.NavigableSet<Long>> completedAhead = new HashMap<>();
  // Per partition revoked or lost, the polled-through offset when it was taken away. A batch whose commit offset is at
  // or below this holds records from an earlier assignment and must not be committed.
  //
  // Known limitation (bounded duplicate work, never data loss): this watermark is not cleared when the same consumer
  // is later re-assigned the partition. If that happens, records the consumer re-reads from the committed offset and
  // re-indexes under the new assignment fall at or below the watermark, so their offsets are treated as stale and not
  // committed. If the partition then goes idle, those offsets stay uncommitted and are re-delivered (and re-indexed,
  // idempotently) on every restart. Delivery stays at-least-once; only the duplicate re-indexing is wasted. It is left
  // as-is deliberately: the safe alternative is per-assignment epoch tracking, and simply clearing the watermark on
  // re-assignment would be unsafe (a record left over from the old assignment could then commit an offset that rewinds
  // another consumer that owned the partition in between). Revisit with epoch tracking only if the duplicate work
  // proves material.
  private final Map<TopicPartition, Long> staleThrough = new HashMap<>();
  // Offsets of completed batches not yet committed successfully; retained and retried if a commit fails.
  private final Map<TopicPartition, OffsetAndMetadata> pendingOffsets = new HashMap<>();

  OffsetCommitTracker(Consumer<?, ?> consumer) {
    this.consumer = consumer;
  }

  /** The rebalance listener to register with the consumer's subscription, so revocations are handled here. */
  ConsumerRebalanceListener rebalanceListener() {
    return new RebalanceListener();
  }

  /** Records that the given partition has been polled through the given offset (the offset after the polled record). */
  void recordPolled(TopicPartition partition, long nextOffset) {
    polledThrough.put(partition, nextOffset);
    // Remember the lowest offset still outstanding on this partition, used to seed the commit frontier.
    lowestPolled.putIfAbsent(partition, nextOffset - 1);
  }

  /**
   * Records the offsets of a completed batch and commits each partition's frontier — the offset below which every
   * record has completed. See the class javadoc for why this cannot skip an uncompleted earlier record.
   */
  void batchCompleted(List<Document> batch) {
    for (Document doc : batch) {
      if (!(doc instanceof KafkaDocument)) {
        continue;
      }
      KafkaDocument kafkaDoc = (KafkaDocument) doc;
      TopicPartition partition = new TopicPartition(kafkaDoc.getTopic(), kafkaDoc.getPartition());
      long offset = kafkaDoc.getOffset();
      // Skip records polled under an earlier assignment of this partition: committing them could rewind a consumer that
      // owns the partition now.
      Long stale = staleThrough.get(partition);
      if (stale != null && offset + 1 <= stale) {
        continue;
      }
      recordCompleted(partition, offset);
    }
    commitPending();
  }

  // Marks one offset complete and advances the partition's frontier across any now-contiguous run of completed offsets.
  private void recordCompleted(TopicPartition partition, long offset) {
    // The frontier starts at the partition's resume position: the lowest offset we have polled on it. The consumer
    // reads contiguously from its committed offset, so nothing below that lowest polled offset is ours to commit.
    long frontier = commitFrontier.computeIfAbsent(partition,
        p -> lowestPolled.getOrDefault(p, offset));
    java.util.NavigableSet<Long> ahead = completedAhead.computeIfAbsent(partition, p -> new java.util.TreeSet<>());
    ahead.add(offset);
    // Absorb completed offsets while they are contiguous from the frontier. This assumes the topic has no offset gaps
    // (non-transactional, non-compacted — see the class javadoc): a true gap would never arrive as a completed offset,
    // so the frontier would stop here and wait for an offset that never comes.
    while (ahead.remove(frontier)) {
      frontier++;
    }
    commitFrontier.put(partition, frontier);
    // The committed offset is the next offset to read, which is exactly the frontier.
    OffsetAndMetadata current = pendingOffsets.get(partition);
    if (current == null || current.offset() < frontier) {
      pendingOffsets.put(partition, new OffsetAndMetadata(frontier));
    }
  }

  /** Commits any offsets of completed batches not yet committed. Called on a clean shutdown. */
  void commitOnClose() {
    if (pendingOffsets.isEmpty()) {
      return;
    }
    try {
      commitPending();
    } catch (Exception e) {
      log.warn("Could not commit completed-batch offsets on close; those documents will be re-delivered.", e);
    }
  }

  // Commits pending offsets for partitions still assigned, dropping the rest. Keeps them if the commit fails, so they
  // are retried later.
  private void commitPending() {
    pendingOffsets.keySet().retainAll(consumer.assignment());
    if (pendingOffsets.isEmpty()) {
      return;
    }
    Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>(pendingOffsets);
    consumer.commitSync(toCommit);
    toCommit.forEach(pendingOffsets::remove);
  }

  // Invoked by the consumer on the indexer thread, from within poll() or close().
  private class RebalanceListener implements ConsumerRebalanceListener {

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      // The partitions are still owned during this callback, so offsets of batches already completed can still be
      // committed before they are handed off.
      Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
      for (TopicPartition partition : partitions) {
        OffsetAndMetadata offset = pendingOffsets.get(partition);
        if (offset != null) {
          toCommit.put(partition, offset);
        }
      }
      if (!toCommit.isEmpty()) {
        try {
          consumer.commitSync(toCommit);
        } catch (Exception e) {
          log.warn("Could not commit offsets {} on revocation; those documents will be re-delivered.", toCommit, e);
        }
      }
      dropAssignmentState(partitions);
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
      // The partitions may already be owned by another consumer, so nothing is committed.
      dropAssignmentState(partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    }

    // Records polled under the current assignment of these partitions become stale, and the partition's per-assignment
    // state (frontier, completed-ahead, pending offset, lowest-polled) is dropped so a later re-assignment starts fresh.
    private void dropAssignmentState(Collection<TopicPartition> partitions) {
      for (TopicPartition partition : partitions) {
        pendingOffsets.remove(partition);
        commitFrontier.remove(partition);
        completedAhead.remove(partition);
        lowestPolled.remove(partition);
        Long polled = polledThrough.remove(partition);
        if (polled != null) {
          staleThrough.merge(partition, polled, Math::max);
        }
      }
    }
  }
}
