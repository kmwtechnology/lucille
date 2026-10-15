package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.KafkaDocument;
import com.typesafe.config.Config;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * The Worker half of a {@link com.kmwllc.lucille.core.WorkerIndexer}: reads documents from the source topic, hands
 * them to the paired Indexer through an in-memory queue, and commits source offsets once the Indexer reports the
 * batches containing them as handled.
 *
 * <p>The messenger is also the rebalance listener of its source consumer. When the group takes partitions away, it
 * waits (up to <code>kafka.rebalanceDrainTimeoutMs</code>) for the Indexer to finish the documents it was handed
 * from those partitions and commits their offsets before giving them up, so the partitions' next owner does not
 * process them again. Without this, every consumer-group membership change (a worker starting or stopping,
 * autoscaling, a rolling deploy) reprocessed whatever was in flight.
 */
public class HybridWorkerMessenger implements WorkerMessenger, ConsumerRebalanceListener {

  /** Default for <code>kafka.rebalanceDrainTimeoutMs</code>. */
  public static final long DEFAULT_REBALANCE_DRAIN_TIMEOUT_MS = 10_000;
  private static final long INDEXER_STOP_CHECK_NANOS = TimeUnit.MILLISECONDS.toNanos(100);

  private static final Logger log = LoggerFactory.getLogger(KafkaWorkerMessenger.class);
  private final Consumer<String, KafkaDocument> sourceConsumer;
  private final KafkaProducer<String, String> kafkaEventProducer;
  private final LinkedBlockingQueue<Document> pipelineDest;
  private final LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets;

  private final Config config;
  private final Duration pollInterval;
  private final String pipelineName;
  private final DeserializationErrorHandler deserializationErrorHandler;
  private final long rebalanceDrainTimeoutMs;

  // The maps below are only touched by the worker thread: the rebalance callbacks run inside sourceConsumer.poll(),
  // which is called from pollDocToProcess() on the same thread as sendForIndexing() and commitPendingDocOffsets().

  // per partition, (offset + 1) of the last record handed to the indexer
  private final Map<TopicPartition, Long> handedOffUpTo = new HashMap<>();
  // per partition, the highest (offset + 1) the indexer has reported as handled
  private final Map<TopicPartition, Long> indexedUpTo = new HashMap<>();
  // offsets reported by the indexer that have not been committed yet
  private final Map<TopicPartition, OffsetAndMetadata> pendingCommit = new HashMap<>();
  // set once the paired indexer is stopping (by WorkerIndexer.stop() from another thread, or by close()): a revocation
  // after that commits what the indexer reported without waiting for more, which would never come
  private volatile boolean indexerStopped = false;

  public HybridWorkerMessenger(Config config, String pipelineName,
      LinkedBlockingQueue<Document> pipelineDest,
      LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets,
      KafkaConsumer sourceConsumer) {
    this.config = config;
    this.pollInterval = KafkaUtils.getPollInterval(config);
    this.pipelineName = pipelineName;
    this.pipelineDest = pipelineDest;
    this.offsets = offsets;
    this.sourceConsumer = sourceConsumer;
    this.kafkaEventProducer = KafkaUtils.createEventProducer(config);
    this.deserializationErrorHandler =
        new DeserializationErrorHandler(config, sourceConsumer);
    this.rebalanceDrainTimeoutMs = getRebalanceDrainTimeoutMs(config);
  }

  public HybridWorkerMessenger(Config config, String pipelineName,
      LinkedBlockingQueue<Document> pipelineDest,
      LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets) {
    this(config, pipelineName, pipelineDest, offsets, createSourceConsumer(config, pipelineName));
    sourceConsumer.subscribe(Pattern.compile(KafkaUtils.getSourceTopicName(pipelineName, config)), this);
  }

  private static KafkaConsumer createSourceConsumer(Config config, String pipelineName) {
    // append random string to kafka client ID to prevent kafka from issuing a warning when multiple consumers
    // with the same client ID are started in separate worker threads
    String kafkaClientId = "com.kmwllc.lucille-worker-" + pipelineName + "-" + RandomStringUtils.randomAlphanumeric(8);
    return KafkaUtils.createDocumentConsumer(config, kafkaClientId);
  }

  private static long getRebalanceDrainTimeoutMs(Config config) {
    long timeoutMs = config.hasPath("kafka.rebalanceDrainTimeoutMs")
        ? config.getLong("kafka.rebalanceDrainTimeoutMs")
        : DEFAULT_REBALANCE_DRAIN_TIMEOUT_MS;
    if (timeoutMs < 0) {
      throw new IllegalArgumentException("kafka.rebalanceDrainTimeoutMs must not be negative, got " + timeoutMs);
    }
    return timeoutMs;
  }

  /**
   * Polls for a document that is waiting to be processed by the pipeline.
   *
   * Does not commit offsets.
   */
  @Override
  public KafkaDocument pollDocToProcess() throws Exception {
    ConsumerRecords<String, KafkaDocument> consumerRecords;
    try {
      consumerRecords = sourceConsumer.poll(pollInterval);
    } catch (RecordDeserializationException e) {
      // rethrows unless configured to skip. The skipped offset is committed along with the next document
      // indexed from this partition; until then, a restart redelivers (and skips) it again.
      deserializationErrorHandler.handleOrRethrow(e);
      return null;
    }
    KafkaUtils.validateAtMostOneRecord(consumerRecords);
    if (consumerRecords.count() > 0) {
      ConsumerRecord<String, KafkaDocument> record = consumerRecords.iterator().next();
      deserializationErrorHandler.onSuccessfulPoll(record);
      KafkaDocument doc = record.value();
      doc.setKafkaMetadata(record);
      return doc;
    }
    return null;
  }

  /**
   * Commits the offsets of all indexer batches completed since the last call, using a single commitSync that keeps
   * only the highest offset per partition.
   *
   * <p>A committed offset is a cumulative watermark: committing N means records 0..N-1 of that partition are done.
   * Committing the highest drained offset therefore has the same effect as committing each drained map in turn, which
   * is what this method did before, and it is safe for the same reasons. Within a WorkerIndexer:
   * <ol>
   *   <li>the source consumer reads one record per poll, in offset order, and this worker processes them one at a
   *   time;</li>
   *   <li>the single indexer thread queues (last offset + 1) for each partition in a batch only once that batch has
   *   been handled: sent to the destination, or reported with FAIL events if sending failed;</li>
   *   <li>batches are marked complete in the order they were formed, and one worker thread drains the queue, so a
   *   partition's offsets are queued in increasing order.</li>
   * </ol>
   * So a map holding offset N for a partition implies every lower offset was already handled. Taking the max also
   * means a stale lower map can never move a partition's committed offset backwards.
   *
   * <p>This relies on (3). What matters is the order in which batches are marked complete, not whether their bulk
   * requests are sent concurrently: completions must follow the order the batches were formed, not the order their
   * requests return. If a later batch were marked complete before an earlier one, its higher offset could be committed,
   * whether merged or one map at a time, while the earlier batch is still in flight, and records from it would be
   * skipped after a restart. Revisit this method if that ordering changes.
   *
   * <p>Offsets are only committed for partitions this consumer currently owns: a report that arrives after its
   * partition was revoked is dropped, as committing it could move the new owner's committed offset backwards. If the
   * commit fails, the offsets stay pending and are retried on the next call.
   */
  @Override
  public void commitPendingDocOffsets() throws Exception {
    if (offsets.isEmpty() && pendingCommit.isEmpty()) {
      return;
    }
    Set<TopicPartition> owned = sourceConsumer.assignment();
    drainReportedOffsets(owned);
    pendingCommit.keySet().retainAll(owned);
    // offsets are committed synchronously, and only after the indexer has sent the batch, so a successful commit means
    // those documents are already in the destination. Doing it synchronously limits how much is reprocessed and
    // reindexed after a HybridWorker crash/restart or a consumer group rebalance.
    commitPending();
  }

  /**
   * Moves the indexer's reports from the offsets queue into {@link #indexedUpTo} and {@link #pendingCommit}, keeping
   * the highest offset per partition and dropping partitions not in <code>owned</code>.
   */
  private void drainReportedOffsets(Set<TopicPartition> owned) {
    Map<TopicPartition, OffsetAndMetadata> batchOffsets;
    while ((batchOffsets = offsets.poll()) != null) {
      mergeReportedOffsets(batchOffsets, owned);
    }
  }

  private void mergeReportedOffsets(Map<TopicPartition, OffsetAndMetadata> batchOffsets, Set<TopicPartition> owned) {
    batchOffsets.forEach((partition, offset) -> {
      if (!owned.contains(partition)) {
        return;
      }
      indexedUpTo.merge(partition, offset.offset(), Math::max);
      pendingCommit.merge(partition, offset,
          (existing, candidate) -> candidate.offset() > existing.offset() ? candidate : existing);
    });
  }

  private void commitPending() {
    if (pendingCommit.isEmpty()) {
      return;
    }
    sourceConsumer.commitSync(new HashMap<>(pendingCommit));
    pendingCommit.clear();
  }

  /**
   * Before giving up partitions, waits for the indexer to report every document handed to it from them, then commits.
   * Gives up after <code>kafka.rebalanceDrainTimeoutMs</code> and commits what has been reported so far; documents
   * not yet reported are processed again by the partitions' next owner. The wait holds up the rebalance for the
   * whole group, so it must stay well below the rebalance timeout (<code>kafka.maxPollIntervalSecs</code>).
   */
  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    if (partitions.isEmpty()) {
      return;
    }
    Set<TopicPartition> owned = new HashSet<>(sourceConsumer.assignment());
    owned.addAll(partitions);
    try {
      awaitIndexer(partitions, owned);
      commitPending();
    } catch (Exception e) {
      log.warn("Could not commit offsets for revoked partitions {}; their uncommitted documents will be processed "
          + "again by the partitions' next owner.", partitions, e);
    } finally {
      forget(partitions);
    }
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
  }

  /**
   * Partitions are lost, rather than revoked, when this consumer has already been removed from the group. Another
   * consumer may own them by now, so nothing is committed.
   */
  @Override
  public void onPartitionsLost(Collection<TopicPartition> partitions) {
    forget(partitions);
  }

  private void awaitIndexer(Collection<TopicPartition> revoked, Set<TopicPartition> owned) {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(rebalanceDrainTimeoutMs);
    drainReportedOffsets(owned);
    while (!indexerCaughtUp(revoked)) {
      long remaining = deadline - System.nanoTime();
      if (indexerStopped || remaining <= 0) {
        log.warn("Indexer did not finish the documents from revoked partitions {} {}; unfinished documents will be "
            + "processed again by the partitions' next owner.", revoked,
            indexerStopped ? "before it stopped" : "within " + rebalanceDrainTimeoutMs + " ms");
        return;
      }
      Map<TopicPartition, OffsetAndMetadata> batchOffsets;
      try {
        // wake up periodically so an indexer stopping mid-wait does not hold the rebalance for the full timeout
        batchOffsets = offsets.poll(Math.min(remaining, INDEXER_STOP_CHECK_NANOS), TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      if (batchOffsets != null) {
        mergeReportedOffsets(batchOffsets, owned);
      }
    }
  }

  private boolean indexerCaughtUp(Collection<TopicPartition> partitions) {
    for (TopicPartition partition : partitions) {
      Long handedOff = handedOffUpTo.get(partition);
      if (handedOff != null && indexedUpTo.getOrDefault(partition, -1L) < handedOff) {
        return false;
      }
    }
    return true;
  }

  private void forget(Collection<TopicPartition> partitions) {
    handedOffUpTo.keySet().removeAll(partitions);
    indexedUpTo.keySet().removeAll(partitions);
    pendingCommit.keySet().removeAll(partitions);
  }

  /**
   * Sends a processed document to the appropriate destination for documents waiting to be indexed.
   *
   */
  @Override
  public void sendForIndexing(Document document) throws Exception {
    pipelineDest.put(document);
    if (document instanceof KafkaDocument) {
      KafkaDocument kafkaDoc = (KafkaDocument) document;
      handedOffUpTo.merge(new TopicPartition(kafkaDoc.getTopic(), kafkaDoc.getPartition()), kafkaDoc.getOffset() + 1,
          Math::max);
    }
  }

  @Override
  public void sendFailed(Document document) throws Exception {
  }

  @Override
  public void sendEvent(Event event) throws Exception {
    if (kafkaEventProducer == null) {
      return;
    }
    String confirmationTopicName = KafkaUtils.getEventTopicName(config, pipelineName, event.getRunId());
    RecordMetadata result = kafkaEventProducer.send(
        new ProducerRecord<>(confirmationTopicName, event.getDocumentId(), event.toString())).get();
  }

  @Override
  public void sendEvent(Document document, String message, Event.Type type) throws Exception {
    if (kafkaEventProducer == null) {
      return;
    }
    Event event = new Event(document, message, type);
    sendEvent(event);
  }

  /**
   * Tells this messenger its paired indexer is stopping, so a later revocation does not wait for reports that will
   * never come. Safe to call from any thread.
   */
  public void indexerStopping() {
    indexerStopped = true;
  }

  @Override
  public void close() throws Exception {
    // closing the consumer revokes its partitions; by now the indexer has stopped
    indexerStopping();
    if (sourceConsumer != null) {
      sourceConsumer.close();
    }
    if (kafkaEventProducer != null) {
      kafkaEventProducer.close();
    }
  }

}

