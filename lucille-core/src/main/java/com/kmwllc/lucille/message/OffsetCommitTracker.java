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
 *   <li><b>No rewind of another consumer.</b> Each partition carries an assignment <i>generation</i>, bumped whenever it
 *   is revoked or lost. A polled document is stamped with the generation it was delivered under; when its batch
 *   completes, the offset is committed only if that generation is still current. So a record polled under an earlier
 *   assignment (the partition having been revoked and possibly reassigned since) is dropped rather than committed, which
 *   would rewind whatever consumer owns the partition now. Dropping an offset only re-delivers its records. This also
 *   makes an eager revoke-and-reassign to the <i>same</i> consumer safe: the re-poll runs under a new generation, so the
 *   stale pre-revoke completions are rejected while the fresh ones commit normally.</li>
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

  // Per partition, the current assignment generation. Bumped each time the partition is revoked or lost, so documents
  // polled before a rebalance carry an older generation than ones polled after, letting batchCompleted reject the
  // stale ones. (Starts at 0 on first poll of a partition.)
  private final Map<TopicPartition, Long> generation = new HashMap<>();
  // Per partition, the offsets polled under the current generation and not yet absorbed into the commit frontier. The
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
   * Records that the given offset was polled on the given partition, and returns the partition's current assignment
   * generation so the caller can stamp it on the document. At completion, {@link #batchCompleted} commits the offset
   * only if the document's stamped generation still matches.
   */
  long recordPolled(TopicPartition partition, long offset) {
    long gen = generation.computeIfAbsent(partition, p -> 0L);
    polled.computeIfAbsent(partition, p -> new TreeSet<>()).add(offset);
    return gen;
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
      // Reject a document whose partition has been revoked (and possibly reassigned) since it was polled: its stamped
      // generation is older than the partition's current one. Committing its offset could rewind whoever owns the
      // partition now. A document that predates generation tracking (-1) and any still-current one is accepted.
      Long current = generation.get(partition);
      if (current != null && kafkaDoc.getDeliveryGeneration() != -1 && kafkaDoc.getDeliveryGeneration() != current) {
        continue;
      }
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
      // Not an outstanding polled offset under the current generation (already absorbed, or stale): nothing to do.
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

    // Bumps each partition's assignment generation and drops its outstanding polled/completed offsets. Any batch still
    // in flight from the old generation will be rejected when it completes (its stamped generation no longer matches),
    // and a re-poll under the new generation starts fresh. Pending (already-computed) commit offsets are left in place
    // so onPartitionsRevoked can still flush them before the handoff.
    private void dropAssignmentState(Collection<TopicPartition> partitions) {
      for (TopicPartition partition : partitions) {
        generation.merge(partition, 1L, Long::sum);
        polled.remove(partition);
        completed.remove(partition);
        // Drop any uncommitted pending offset too: onPartitionsRevoked already flushed it above (while the partition
        // was still owned); on the lost path nothing was committed, and keeping it could rewind the new owner.
        pendingOffsets.remove(partition);
      }
    }
  }
}
