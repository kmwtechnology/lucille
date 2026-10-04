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
 * <p> Correctness, given that the Indexer completes batches in the order it dispatched them:
 * <ul>
 *   <li><b>No lost document.</b> Every record polled before this batch's last record went into this batch or an earlier
 *   one, so when this batch completes, every record below its committed offset has been indexed. A crash re-reads from
 *   the committed offset and loses nothing.</li>
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

  // Per assigned partition, the offset after the last record polled in the current assignment.
  private final Map<TopicPartition, Long> polledThrough = new HashMap<>();
  // Per partition revoked or lost, the polled-through offset when it was taken away. A batch whose commit offset is at
  // or below this holds records from an earlier assignment and must not be committed.
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
  }

  /** Records the offsets of a completed batch and commits all pending offsets. See the class javadoc for correctness. */
  void batchCompleted(List<Document> batch) {
    for (Document doc : batch) {
      if (!(doc instanceof KafkaDocument)) {
        continue;
      }
      KafkaDocument kafkaDoc = (KafkaDocument) doc;
      TopicPartition partition = new TopicPartition(kafkaDoc.getTopic(), kafkaDoc.getPartition());
      // The committed offset is the offset of the next record to read, so add one to the last record processed.
      long nextOffset = kafkaDoc.getOffset() + 1;
      // Skip records polled under an earlier assignment of this partition: committing them could rewind a consumer that
      // owns the partition now.
      Long stale = staleThrough.get(partition);
      if (stale != null && nextOffset <= stale) {
        continue;
      }
      OffsetAndMetadata current = pendingOffsets.get(partition);
      if (current == null || current.offset() < nextOffset) {
        pendingOffsets.put(partition, new OffsetAndMetadata(nextOffset));
      }
    }
    commitPending();
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
      forget(partitions);
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
      // The partitions may already be owned by another consumer, so nothing is committed.
      forget(partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    }

    // Records polled under the current assignment of these partitions become stale, and any pending offset is dropped.
    private void forget(Collection<TopicPartition> partitions) {
      for (TopicPartition partition : partitions) {
        pendingOffsets.remove(partition);
        Long polled = polledThrough.remove(partition);
        if (polled != null) {
          staleThrough.merge(partition, polled, Math::max);
        }
      }
    }
  }
}
