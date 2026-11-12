package com.kmwllc.lucille.message;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.EventFieldConfig;
import com.kmwllc.lucille.core.KafkaDocument;
import com.kmwllc.lucille.util.LogUtils;
import com.typesafe.config.Config;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * A WorkerMessenger for hybrid mode: reads documents from a Kafka source topic and hands them to a co-located Indexer
 * through an in-memory queue. Its {@link HybridIndexerMessenger} reports each batch back once it completes, and the
 * source offsets are committed by an {@link OffsetCommitTracker}, as the standalone Indexer commits destination offsets:
 * a frontier that never passes an unfinished record and never moves backwards across a rebalance. The tracker also
 * answers the paired Indexer's ownership check before each send.
 *
 * <p> A record is finished once the worker has processed it and every document it sent for indexing (the record itself,
 * children a stage created, or both) has been reported complete. A record that sends nothing, because the pipeline
 * dropped or failed it or it went to the dead letter queue, is finished as soon as the worker is done with it, which is
 * when the worker polls again or commits. For one record, the sequence is:
 * <ol>
 *   <li>{@link #pollDocToProcess()} first finishes the worker's processing of the previous record (step 4), then polls
 *   one record, records its offset in the tracker as polled, and starts the record's count at 1, for the worker's
 *   processing of it.</li>
 *   <li>As the worker runs the pipeline, each {@link #sendForIndexing} queues a document for the Indexer, notes which
 *   record it came from, and adds 1 to that record's count.</li>
 *   <li>On the Indexer's thread, {@code batchCompleted} only queues the batch for the worker thread. Nothing is counted
 *   or committed yet.</li>
 *   <li>{@link #commitPendingDocOffsets()}, which the worker calls after each record, subtracts the 1 for the worker's
 *   processing, then applies the queued batches, subtracting 1 for each document in them. A record whose count reaches
 *   0 is finished, and the tracker marks its offset complete. Finally the tracker commits its frontier, if it moved.</li>
 * </ol>
 * The count can't reach 0 while the worker is still sending a record's documents, because the 1 for its processing is
 * only subtracted after the worker has sent them all, and a batch is only applied on the worker thread, after the sends
 * it reports were counted.
 *
 * <p> The consumer, the tracker and the rebalance callbacks all run on the worker thread. The Indexer's reports cross
 * threads through a queue that the worker thread drains before each poll, before each commit and on revocation.
 *
 * <p> When partitions are revoked, lost or rewound, the documents from them that are still waiting in the queue,
 * children included, are removed, so that the partition's next owner processes and indexes them instead of both
 * consumers doing so. Each such partition also starts a new epoch. A record polled in an earlier epoch can no longer
 * finish, and the ownership check fails for every document sent from it, children included, including any the Indexer
 * has already taken into a batch.
 *
 * <p> Internal to Lucille: {@link com.kmwllc.lucille.core.WorkerIndexer} creates it, sharing its document queue with a
 * {@link HybridIndexerMessenger}. It is public only because the two are in different packages.
 *
 * <p> Registers the Worker consumer's poll and commit metrics (see {@link KafkaConsumerMetrics}) and, in the shared
 * registry ({@link LogUtils#METRICS_REG}), {@code <pipeline>.hybrid.indexerQueue.full} (Meter): documents that found the
 * indexer queue full and had to wait for space. It is shared by every WorkerIndexer pair running
 * the pipeline in this JVM, so the value covers all of them.
 */
public class HybridWorkerMessenger implements WorkerMessenger {

  private static final Logger log = LoggerFactory.getLogger(HybridWorkerMessenger.class);

  public static final String INDEXER_QUEUE_FULL_SUFFIX = ".hybrid.indexerQueue.full";

  // at most one "indexer queue is full" warning per this interval
  static final long QUEUE_FULL_WARN_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(60);

  private final Consumer<String, KafkaDocument> sourceConsumer;
  private final KafkaProducer<String, String> kafkaEventProducer;
  // only created when the fail topic is enabled, so a worker that doesn't use it needs no document producer config
  private final KafkaProducer<String, Document> failTopicProducer;
  private final BlockingQueue<Document> pipelineDest;
  private final OffsetCommitTracker offsetCommitTracker;
  private final ConsumerRebalanceListener trackerListener;
  // The paired Indexer's reports of completed batches and of batches it did not send, waiting for the worker thread
  private final Queue<BatchResult> batchResults = new ConcurrentLinkedQueue<>();

  // The record the worker is processing: returned by the last poll, which the worker has not yet followed with another
  // poll or a commit. Worker thread only.
  private PolledRecord current;
  // The record each document sent for indexing came from, by identity, since a child a stage created has no Kafka
  // metadata of its own. Removed once the Indexer has reported the document. Written on the worker thread; also read by
  // the ownership check, from the Indexer's send threads.
  private final Map<Document, PolledRecord> outputs = Collections.synchronizedMap(new IdentityHashMap<>());
  // Per partition, incremented each time the partition is revoked, lost or rewound. Absent means 0.
  private final Map<TopicPartition, Integer> epochs = new ConcurrentHashMap<>();

  // As in KafkaIndexerMessenger: once the worker has gone max.poll.interval.ms without polling it has left its group, so
  // ownsInput stops confirming ownership. Written on the worker thread, read from the Indexer's send threads.
  private final long maxPollIntervalNanos;
  private volatile long lastReturnedPollStartNanos = System.nanoTime();

  private final Config config;
  private final Duration pollInterval;
  private final String pipelineName;
  private final EventFieldConfig eventFieldConfig;
  private final DeserializationErrorHandler deserializationErrorHandler;

  private final KafkaConsumerMetrics consumerMetrics;
  private final Meter indexerQueueFull;
  // only the worker thread calls sendForIndexing, so this needs no synchronization
  private long lastQueueFullWarnNanos;
  private boolean warnedQueueFull = false;

  // takes the source consumer, so tests can supply one
  HybridWorkerMessenger(Config config, String pipelineName,
      BlockingQueue<Document> pipelineDest,
      Consumer<String, KafkaDocument> sourceConsumer) {
    this.config = config;
    this.pollInterval = KafkaUtils.getPollInterval(config);
    this.maxPollIntervalNanos = KafkaUtils.getMaxPollInterval(config).toNanos();
    this.pipelineName = pipelineName;
    this.pipelineDest = pipelineDest;
    this.sourceConsumer = sourceConsumer;
    this.consumerMetrics = KafkaConsumerMetrics.forWorker(pipelineName);
    this.offsetCommitTracker = new OffsetCommitTracker(sourceConsumer, consumerMetrics);
    this.trackerListener = offsetCommitTracker.rebalanceListener();
    sourceConsumer.subscribe(Pattern.compile(KafkaUtils.getSourceTopicName(pipelineName, config)), new RebalanceListener());
    this.kafkaEventProducer = KafkaUtils.createEventProducer(config);
    this.failTopicProducer = KafkaUtils.workerFailTopicEnabled(config) ? KafkaUtils.createDocumentProducer(config) : null;
    this.eventFieldConfig = EventFieldConfig.fromConfig(config);
    this.deserializationErrorHandler =
        new DeserializationErrorHandler(config, sourceConsumer);

    MetricRegistry metrics = SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG);
    this.indexerQueueFull = metrics.meter(pipelineName + INDEXER_QUEUE_FULL_SUFFIX);
  }

  public HybridWorkerMessenger(Config config, String pipelineName, BlockingQueue<Document> pipelineDest) {
    this(config, pipelineName, pipelineDest, createSourceConsumer(config, pipelineName));
  }

  private static Consumer<String, KafkaDocument> createSourceConsumer(Config config, String pipelineName) {
    // append random string to kafka client ID to prevent kafka from issuing a warning when multiple consumers
    // with the same client ID are started in separate worker threads
    String kafkaClientId = "com.kmwllc.lucille-worker-" + pipelineName + "-" + RandomStringUtils.randomAlphanumeric(8);
    return KafkaUtils.createDocumentConsumer(config, kafkaClientId);
  }

  /**
   * Polls for a document that is waiting to be processed by the pipeline.
   *
   * Does not commit offsets.
   */
  @Override
  public KafkaDocument pollDocToProcess() throws MessengerException {
    // the worker polls again only once it is done with the previous record
    finishCurrent();
    // so that offsets the Indexer has reported as not sent can be checked by rewindUnsent below
    drainBatchResults();
    ConsumerRecords<String, KafkaDocument> consumerRecords;
    long pollStartNanos = System.nanoTime();
    try (Timer.Context ignored = consumerMetrics.timePoll()) {
      consumerRecords = sourceConsumer.poll(pollInterval);
    } catch (RecordDeserializationException e) {
      // throws unless configured to skip. The skipped offset is never recorded as polled, so the commit frontier moves
      // past it once the surrounding documents complete; until then, a restart redelivers (and skips) it again.
      deserializationErrorHandler.handleOrThrow(e);
      return null;
    } finally {
      lastReturnedPollStartNanos = pollStartNanos;
    }
    KafkaUtils.validateAtMostOneRecord(consumerRecords);
    // Any rebalance callback has run, so ownership of the partitions of batches that were not sent is now known.
    Set<TopicPartition> rewound = offsetCommitTracker.rewindUnsent();
    // the queued documents of a rewound partition will be polled again
    purge(rewound);
    if (consumerRecords.count() > 0) {
      ConsumerRecord<String, KafkaDocument> record = consumerRecords.iterator().next();
      TopicPartition partition = new TopicPartition(record.topic(), record.partition());
      if (rewound.contains(partition)) {
        // The partition was just seeked back below this record, which will be polled again in order.
        return null;
      }
      deserializationErrorHandler.onSuccessfulPoll(record);
      KafkaDocument doc = record.value();
      doc.setKafkaMetadata(record.topic(), record.partition(), record.offset(), record.key());
      offsetCommitTracker.recordPolled(partition, record.offset());
      current = new PolledRecord(partition, record.offset(), epoch(partition));
      return doc;
    }
    return null;
  }

  /**
   * Marks the record the worker last polled as processed, then commits the source offsets of the records finished since
   * the last call, once, up to the frontier below which every polled record has finished (see
   * {@link OffsetCommitTracker}). A failed commit is kept and retried on the next call.
   *
   * <p>The commit is timed, and a commit that throws is counted before the exception is rethrown (see
   * {@link KafkaConsumerMetrics}).
   */
  @Override
  public void commitPendingDocOffsets() {
    finishCurrent();
    drainBatchResults();
    offsetCommitTracker.commitPending();
  }

  /** Called by the paired Indexer, on its thread, when a batch completes. */
  void batchCompleted(List<Document> batch) {
    queueBatchResult(true, batch);
  }

  /** Called by the paired Indexer, on its thread, for a batch it did not send because it could not confirm ownership. */
  void batchNotSent(List<Document> batch) {
    queueBatchResult(false, batch);
  }

  /**
   * Whether the paired Indexer may still send the given document: false once the worker has gone longer than
   * max.poll.interval.ms without polling, or once the partition of the record the document came from was revoked, lost
   * or rewound since that record was polled. This applies to a child a stage created as to the record itself. Called
   * from the Indexer's send threads.
   */
  boolean ownsInput(Document doc) {
    if (System.nanoTime() - lastReturnedPollStartNanos >= maxPollIntervalNanos) {
      return false;
    }
    PolledRecord source = outputs.get(doc);
    if (source != null) {
      return isCurrent(source);
    }
    // not tracked, such as a document removed from the queue when its partition was given up: checked by its own offset
    return !(doc instanceof KafkaDocument kafkaDoc)
        || offsetCommitTracker.isOutstanding(new TopicPartition(kafkaDoc.getTopic(), kafkaDoc.getPartition()), kafkaDoc.getOffset());
  }

  private void queueBatchResult(boolean sent, List<Document> batch) {
    // matched to records on the worker thread, which owns that state
    batchResults.add(new BatchResult(sent, new ArrayList<>(batch)));
  }

  // Applies the Indexer's reports to the records they came from. On the worker thread.
  private void drainBatchResults() {
    BatchResult result;
    while ((result = batchResults.poll()) != null) {
      for (Document doc : result.documents()) {
        PolledRecord source = outputs.remove(doc);
        if (source == null) {
          // removed from the queue when its partition was given up, or already reported
          continue;
        }
        if (result.sent()) {
          release(source);
        } else if (isCurrent(source)) {
          // Not released, so the record stays unfinished: its partition is rewound to it after the next poll.
          offsetCommitTracker.recordNotSent(source.partition, source.offset);
        }
      }
    }
  }

  // The worker is done with the record it last polled.
  private void finishCurrent() {
    if (current != null) {
      PolledRecord source = current;
      current = null;
      release(source);
    }
  }

  // One thing the record was waiting for is done. Once nothing is left, the record is finished and its offset completes,
  // unless its partition has started a new epoch since it was polled, in which case the record will be processed again.
  private void release(PolledRecord source) {
    if (--source.count == 0 && isCurrent(source)) {
      offsetCommitTracker.recordCompleted(source.partition, source.offset);
    }
  }

  private int epoch(TopicPartition partition) {
    return epochs.getOrDefault(partition, 0);
  }

  // Whether the record was polled in its partition's current epoch.
  private boolean isCurrent(PolledRecord source) {
    return source.epoch == epoch(source.partition);
  }

  // The partition of the record a document came from, or null if it is not tracked.
  private TopicPartition partitionOf(Document doc) {
    PolledRecord source = outputs.get(doc);
    if (source != null) {
      return source.partition;
    }
    return doc instanceof KafkaDocument kafkaDoc ? new TopicPartition(kafkaDoc.getTopic(), kafkaDoc.getPartition()) : null;
  }

  // Removes the waiting documents of the given partitions from the queue, children included: they were not indexed and
  // their offsets are not committed, so whoever polls the partition next indexes them. Then starts a new epoch for each
  // partition, so a document the Indexer has already taken fails the ownership check, and a late report can't finish a
  // record polled before.
  private void purge(Collection<TopicPartition> partitions) {
    if (partitions.isEmpty()) {
      return;
    }
    pipelineDest.removeIf(doc -> {
      if (!partitions.contains(partitionOf(doc))) {
        return false;
      }
      // never taken by the Indexer, so never reported
      outputs.remove(doc);
      return true;
    });
    for (TopicPartition partition : partitions) {
      epochs.merge(partition, 1, Integer::sum);
    }
  }

  private record BatchResult(boolean sent, List<Document> documents) {
  }

  // A polled record: where it is, the epoch of its partition when it was polled, and how many things it is waiting for,
  // which are the worker's processing of it and each document sent for indexing from it that the Indexer has not
  // reported. The count is used on the worker thread only.
  private static final class PolledRecord {
    private final TopicPartition partition;
    private final long offset;
    private final int epoch;
    private int count = 1;

    private PolledRecord(TopicPartition partition, long offset, int epoch) {
      this.partition = partition;
      this.offset = offset;
      this.epoch = epoch;
    }
  }

  // Invoked by the consumer on the worker thread, from within poll() or close().
  private class RebalanceListener implements ConsumerRebalanceListener {

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      // record the batches completed so far, so the tracker commits them before the partitions are handed off
      drainBatchResults();
      purge(partitions);
      trackerListener.onPartitionsRevoked(partitions);
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
      purge(partitions);
      trackerListener.onPartitionsLost(partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
      trackerListener.onPartitionsAssigned(partitions);
    }
  }

  /**
   * Sends a processed document to the appropriate destination for documents waiting to be indexed, and notes the polled
   * record it came from, which is not finished until the Indexer reports the document.
   *
   * <p>If the indexer queue is full, marks <code>indexerQueue.full</code>, logs a warning (at most once a minute) and
   * blocks until there is space.
   */
  @Override
  public void sendForIndexing(Document document) throws MessengerException {
    if (!pipelineDest.offer(document)) {
      indexerQueueFull.mark();
      long now = System.nanoTime();
      if (!warnedQueueFull || now - lastQueueFullWarnNanos >= QUEUE_FULL_WARN_INTERVAL_NANOS) {
        warnedQueueFull = true;
        lastQueueFullWarnNanos = now;
        log.warn("Indexer queue for pipeline {} is full ({} documents); the worker is waiting for the indexer. "
            + "This warning is logged at most once a minute; see the {} meter for a count.",
            pipelineName, pipelineDest.size(), pipelineName + INDEXER_QUEUE_FULL_SUFFIX);
      }
      try {
        pipelineDest.put(document);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new MessengerException("Interrupted while queueing document " + document.getId() + " for indexing", e);
      }
    }
    // The Indexer may already have taken it, but its report is only applied on this thread, after this. A document sent
    // twice is counted once.
    if (current != null && outputs.putIfAbsent(document, current) == null) {
      current.count++;
    }
  }

  @Override
  public void sendFailed(Document document) throws MessengerException {
    KafkaUtils.sendToFailTopic(failTopicProducer, pipelineName, document);
  }

  @Override
  public void sendEvent(Event event) throws MessengerException {
    if (kafkaEventProducer == null) {
      return;
    }
    String confirmationTopicName = KafkaUtils.getEventTopicName(config, pipelineName, event.getRunId());
    try {
      kafkaEventProducer.send(
          new ProducerRecord<>(confirmationTopicName, event.getDocumentId(), event.toString())).get();
    } catch (ExecutionException e) {
      throw KafkaUtils.toMessengerException("Sending " + event.getType() + " event for document " + event.getDocumentId(), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new MessengerException("Sending " + event.getType() + " event for document " + event.getDocumentId() + " was interrupted", e);
    }
  }

  @Override
  public void sendEvent(Document document, String message, Event.Type type) throws MessengerException {
    if (kafkaEventProducer == null) {
      return;
    }
    Event event = new Event(document, message, type, eventFieldConfig);
    sendEvent(event);
  }

  @Override
  public void close() {
    if (sourceConsumer != null) {
      // commit the batches completed but not yet committed, so a clean shutdown does not force their re-delivery
      drainBatchResults();
      offsetCommitTracker.commitOnClose();
      sourceConsumer.close();
    }
    if (kafkaEventProducer != null) {
      kafkaEventProducer.close();
    }
    if (failTopicProducer != null) {
      failTopicProducer.close();
    }
  }

}

