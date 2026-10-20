package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.KafkaDocument;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
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
 * <p> Correctness:
 * <ul>
 *   <li><b>No lost document.</b> Per partition, the committed offset is a <i>frontier</i> that advances only across the
 *   polled offsets that have completed, in order, so every record below it has been indexed. It never passes an
 *   uncompleted earlier record — which can arise from out-of-order completion generally, and specifically from
 *   MultiBatch, where per-index sub-batches let a higher-offset record complete while a lower-offset one is still
 *   unflushed. The frontier advances across the <i>sequence of offsets actually polled</i>, not by {@code offset + 1},
 *   so a gap between polled offsets (a transaction marker, or a record removed by compaction) does not stall it. A
 *   crash re-reads from the frontier and loses nothing. (This holds independently of the order batches complete in.)</li>
 *   <li><b>No rewind, no stall across rebalances.</b> The frontier advances only across offsets polled since the
 *   partition was last assigned to this consumer: revoking or losing a partition drops all of its tracked offsets, and a
 *   partition that comes back is polled again from its committed offset. So a commit never goes below the committed
 *   offset at reassignment, and only partitions still assigned are committed, so no other consumer is rewound. A batch
 *   polled before a rebalance that completes afterwards either matches no tracked offset and is ignored, or matches an
 *   offset polled again since (the same record, since Kafka records never change) and counts as completing it: that
 *   record was processed by the earlier send, so the frontier may pass it (Indexer.completeBatch calls batchComplete only
 *   for a batch whose documents were all processed, indexed or failed with FAIL events, and never after an Error), and
 *   the re-polled copy's later completion is redundant. Either way a stale completion can only move the frontier forward, never hold it back, so an eager
 *   revoke-and-reassign to the <i>same</i> consumer cannot stall it. (With the Indexer's pre-revocation drain, a clean
 *   revoke leaves no batch in flight, so stale completions arise only after a partition is lost.)</li>
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

  // Run at the start of onPartitionsRevoked, before offsets are committed: the Indexer flushes its open batch and
  // drains in-flight batches so their offsets reach pendingOffsets and are committed before the partition is handed
  // off. Null until the Indexer registers it (and when there is no Indexer, e.g. in some tests).
  private Runnable preRevocationHook;

  // Per partition, the offsets polled since it was last assigned and not yet absorbed into the commit frontier. The
  // frontier advances across this set (not by offset+1), so gaps between polled offsets never stall it.
  private final Map<TopicPartition, NavigableSet<Long>> polled = new HashMap<>();
  // Per partition, polled offsets whose batch has completed, not yet absorbed into the frontier.
  private final Map<TopicPartition, NavigableSet<Long>> completed = new HashMap<>();
  // Offsets of completed batches not yet committed successfully; retained and retried if a commit fails. The value is
  // the next offset to read (exclusive upper bound), matching Kafka's committed-offset semantics.
  private final Map<TopicPartition, OffsetAndMetadata> pendingOffsets = new HashMap<>();

  OffsetCommitTracker(Consumer<?, ?> consumer) {
    this.consumer = consumer;
  }

  /** The rebalance listener to register with the consumer's subscription, so revocations are handled here. */
  ConsumerRebalanceListener rebalanceListener() {
    return new RebalanceListener();
  }

  /**
   * Registers a hook run at the start of {@link RebalanceListener#onPartitionsRevoked} — before offsets are committed —
   * so the Indexer can flush its open batch and drain in-flight batches, letting their offsets be committed before the
   * partition is handed off. Without it, a clean rebalance re-delivers the documents still in the open batch or in
   * flight. See {@link IndexerMessenger#setPreRevocationHook}.
   */
  void setPreRevocationHook(Runnable hook) {
    this.preRevocationHook = hook;
  }

  /** Records that the given offset was polled on the given partition, so the frontier can advance across it. */
  void recordPolled(TopicPartition partition, long offset) {
    polled.computeIfAbsent(partition, p -> new TreeSet<>()).add(offset);
  }

  /**
   * Records the offsets of a completed batch and commits each partition's frontier — the offset below which every
   * polled record has completed. See the class javadoc for why this cannot skip an uncompleted earlier record and why
   * it is safe across rebalances.
   */
  void batchCompleted(List<Document> batch) {
    for (Document doc : batch) {
      if (!(doc instanceof KafkaDocument)) {
        continue;
      }
      KafkaDocument kafkaDoc = (KafkaDocument) doc;
      TopicPartition partition = new TopicPartition(kafkaDoc.getTopic(), kafkaDoc.getPartition());
      recordCompleted(partition, kafkaDoc.getOffset());
    }
    commitPending();
  }

  // Marks one polled offset complete and advances the partition's frontier across the leading run of polled offsets
  // that have now completed. The frontier walks the polled sequence rather than offset+1, so a gap between polled
  // offsets (compaction, transaction markers) does not stall it.
  private void recordCompleted(TopicPartition partition, long offset) {
    NavigableSet<Long> polledOffsets = polled.get(partition);
    if (polledOffsets == null || !polledOffsets.contains(offset)) {
      // Not an outstanding polled offset (already absorbed, or polled before a revoke and not polled again): nothing to
      // do.
      return;
    }
    completed.computeIfAbsent(partition, p -> new TreeSet<>()).add(offset);
    NavigableSet<Long> done = completed.get(partition);
    // Absorb polled offsets, lowest first, while each has completed. Stop at the first polled-but-not-completed offset:
    // the frontier must not pass it.
    long commitOffset = -1;
    while (!polledOffsets.isEmpty() && done.contains(polledOffsets.first())) {
      long absorbed = polledOffsets.pollFirst();
      done.remove(absorbed);
      commitOffset = absorbed + 1;
    }
    if (commitOffset == -1) {
      return;
    }
    // The committed offset is the next offset to read: one past the highest contiguous completed polled offset.
    OffsetAndMetadata pending = pendingOffsets.get(partition);
    if (pending == null || pending.offset() < commitOffset) {
      pendingOffsets.put(partition, new OffsetAndMetadata(commitOffset));
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
      // Flush the open batch and drain in-flight batches first (on the indexer thread, which this callback runs on, so
      // it owns the batch/dispatcher). That completes every polled-but-unfinished document — emitting its events and
      // advancing its offset into pendingOffsets below — while the partition is still owned, so the commit below hands
      // off a frontier past them. Without this, those documents are re-delivered after the rebalance and indexed (and
      // FINISHed) again. It runs before dropAssignmentState drops the partitions' tracked offsets, so the just-completed
      // documents still advance the frontier rather than being ignored.
      if (preRevocationHook != null) {
        preRevocationHook.run();
      }

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

    // Drops all of each partition's outstanding state: polled, completed, and pending commit offsets. A batch still in
    // flight from before will find its offsets untracked when it completes, unless they have been polled again since
    // (see the class javadoc), and a re-poll starts fresh from the committed offset. The pending commit offset is
    // dropped here too; on the revoke path onPartitionsRevoked has already flushed it (while the partition was still
    // owned) before calling this, and on the lost path nothing was committed and keeping it could rewind the new owner.
    private void dropAssignmentState(Collection<TopicPartition> partitions) {
      for (TopicPartition partition : partitions) {
        polled.remove(partition);
        completed.remove(partition);
        pendingOffsets.remove(partition);
      }
    }
  }
}
