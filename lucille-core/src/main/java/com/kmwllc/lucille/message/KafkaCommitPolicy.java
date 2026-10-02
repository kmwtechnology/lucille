package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.KafkaDocument;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * When {@link KafkaIndexerMessenger} commits offsets on the destination topic. {@link #beforePoll()},
 * {@link #afterRecordPolled()}, and {@link #close()} are called on the thread that owns the consumer;
 * {@link #batchComplete(List)} may be called on any thread.
 */
interface KafkaCommitPolicy {

  /** Called before each poll of the consumer. */
  void beforePoll();

  /** Called after a poll that returned a record. */
  void afterRecordPolled();

  /** Called when the Indexer has completed a batch: sent it and emitted its events. */
  void batchComplete(List<Document> batch);

  /** Called before the consumer is closed. Best effort: must not throw. */
  void close();

  /** Commit on poll when indexer.maxConcurrentBatches is 1, and on batch completion when it is greater. */
  static KafkaCommitPolicy forMaxConcurrentBatches(int maxConcurrentBatches, Consumer<?, ?> consumer) {
    return maxConcurrentBatches > 1 ? new CommitOnBatchCompletion(consumer) : new CommitOnPoll(consumer);
  }

  /**
   * Commits each record's offset as soon as it is polled, before it is indexed. The indexer polls nothing while it sends
   * a batch, so a crash can lose (never index) at most about one batch.
   */
  final class CommitOnPoll implements KafkaCommitPolicy {

    private final Consumer<?, ?> consumer;

    CommitOnPoll(Consumer<?, ?> consumer) {
      this.consumer = consumer;
    }

    @Override
    public void beforePoll() {
    }

    @Override
    public void afterRecordPolled() {
      // offsets are committed synchronously to ensure that offsets are successfully committed and to reduce the likelihood of duplicate events being sent to the event topic.
      // This reduces the number of documents that might be reindexed in the event of an indexer crash/restart or in the case of a consumer group reblance.
      consumer.commitSync();
    }

    @Override
    public void batchComplete(List<Document> batch) {
    }

    @Override
    public void close() {
    }
  }

  /**
   * Commits offsets only once the batches holding their records have completed, so delivery is at-least-once: a crash or
   * rebalance redelivers in-flight records (duplicate indexing and completion events) but never loses them.
   *
   * <p> {@link #batchComplete(List)} only records, per partition, the offset after the batch's last record. The recorded
   * offsets are committed at the start of the next poll, on the thread that owns the consumer. Committing offset N is
   * safe because the Indexer completes batches in the order it sent them, and every record polled before N on that
   * partition went into this batch or an earlier one.
   *
   * <p> Only partitions in the consumer's current assignment are committed; offsets for others are dropped, so their
   * records are redelivered to the partition's new owner. A failed commit is logged and retried at the next poll.
   */
  final class CommitOnBatchCompletion implements KafkaCommitPolicy {

    private static final Logger log = LoggerFactory.getLogger(CommitOnBatchCompletion.class);

    private final Consumer<?, ?> consumer;
    // Next offset to commit per partition. Concurrent, and merged with max, so completion may run on any thread.
    private final Map<TopicPartition, Long> pending = new ConcurrentHashMap<>();

    CommitOnBatchCompletion(Consumer<?, ?> consumer) {
      this.consumer = consumer;
    }

    @Override
    public void beforePoll() {
      commitPending();
    }

    @Override
    public void afterRecordPolled() {
    }

    @Override
    public void batchComplete(List<Document> batch) {
      for (Document doc : batch) {
        if (doc instanceof KafkaDocument kDoc) {
          pending.merge(new TopicPartition(kDoc.getTopic(), kDoc.getPartition()), kDoc.getOffset() + 1, Math::max);
        }
      }
    }

    @Override
    public void close() {
      try {
        commitPending();
      } catch (Exception e) {
        log.warn("Could not commit offsets of completed batches on close; those documents will be delivered again.", e);
      }
    }

    private void commitPending() {
      if (pending.isEmpty()) {
        return;
      }
      Set<TopicPartition> assigned = consumer.assignment();
      Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
      for (Map.Entry<TopicPartition, Long> entry : Map.copyOf(pending).entrySet()) {
        if (assigned.contains(entry.getKey())) {
          toCommit.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
        } else {
          pending.remove(entry.getKey(), entry.getValue());
        }
      }
      if (toCommit.isEmpty()) {
        return;
      }
      try {
        consumer.commitSync(toCommit);
      } catch (InterruptException e) {
        throw e;
      } catch (KafkaException e) {
        log.warn("Could not commit offsets {}; retrying at the next poll.", toCommit, e);
        return;
      }
      // remove only what was committed: a batch completed meanwhile may have recorded a later offset
      toCommit.forEach((partition, offset) -> pending.remove(partition, offset.offset()));
    }
  }
}
