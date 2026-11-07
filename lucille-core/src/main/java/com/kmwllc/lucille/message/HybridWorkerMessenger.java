package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.EventFieldConfig;
import com.kmwllc.lucille.core.KafkaDocument;
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
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Pattern;

/**
 * A WorkerMessenger for hybrid mode: reads documents from a Kafka source topic and hands them to a co-located Indexer
 * through an in-memory queue. Its {@link HybridIndexerMessenger} reports each batch back once it completes, and the
 * source offsets are committed by an {@link OffsetCommitTracker}, as the standalone Indexer commits destination offsets:
 * a frontier that never passes an unfinished record and never moves backwards across a rebalance. The tracker also
 * answers the paired Indexer's ownership check before each send.
 *
 * <p> The consumer, the tracker and the rebalance callbacks all run on the worker thread. The Indexer's reports cross
 * threads through a queue that the worker thread drains before each poll, before each commit and on revocation.
 *
 * <p> When partitions are revoked or lost, the documents from them that are still waiting in the queue are removed, so
 * that the partition's next owner processes and indexes them instead of both consumers doing so.
 *
 * <p> Internal to Lucille: {@link com.kmwllc.lucille.core.WorkerIndexer} creates it, sharing its document queue with a
 * {@link HybridIndexerMessenger}. It is public only because the two are in different packages.
 */
public class HybridWorkerMessenger implements WorkerMessenger {

  private static final Logger log = LoggerFactory.getLogger(KafkaWorkerMessenger.class);
  private final Consumer<String, KafkaDocument> sourceConsumer;
  private final KafkaProducer<String, String> kafkaEventProducer;
  private final BlockingQueue<Document> pipelineDest;
  private final OffsetCommitTracker offsetCommitTracker;
  private final ConsumerRebalanceListener trackerListener;
  // The paired Indexer's reports of completed batches and of batches it did not send, waiting for the worker thread
  private final Queue<BatchResult> batchResults = new ConcurrentLinkedQueue<>();

  // As in KafkaIndexerMessenger: once the worker has gone max.poll.interval.ms without polling it has left its group, so
  // ownsInput stops confirming ownership. Written on the worker thread, read from the Indexer's send threads.
  private final long maxPollIntervalNanos;
  private volatile long lastReturnedPollStartNanos = System.nanoTime();

  private final Config config;
  private final Duration pollInterval;
  private final String pipelineName;
  private final EventFieldConfig eventFieldConfig;
  private final DeserializationErrorHandler deserializationErrorHandler;

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
    this.offsetCommitTracker = new OffsetCommitTracker(sourceConsumer);
    this.trackerListener = offsetCommitTracker.rebalanceListener();
    sourceConsumer.subscribe(Pattern.compile(KafkaUtils.getSourceTopicName(pipelineName, config)), new RebalanceListener());
    this.kafkaEventProducer = KafkaUtils.createEventProducer(config);
    this.eventFieldConfig = EventFieldConfig.fromConfig(config);
    this.deserializationErrorHandler =
        new DeserializationErrorHandler(config, sourceConsumer);
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
  public KafkaDocument pollDocToProcess() {
    // so that offsets the Indexer has reported as not sent can be checked by rewindUnsent below
    drainBatchResults();
    ConsumerRecords<String, KafkaDocument> consumerRecords;
    long pollStartNanos = System.nanoTime();
    try {
      consumerRecords = sourceConsumer.poll(pollInterval);
    } catch (RecordDeserializationException e) {
      // rethrows unless configured to skip. The skipped offset is never recorded as polled, so the commit frontier moves
      // past it once the surrounding documents complete; until then, a restart redelivers (and skips) it again.
      deserializationErrorHandler.handleOrRethrow(e);
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
      return doc;
    }
    return null;
  }

  /**
   * Commits the source offsets of the batches the paired Indexer has completed since the last call, once, up to the
   * frontier below which every polled record has completed (see {@link OffsetCommitTracker}). A failed commit is kept and
   * retried on the next call.
   */
  @Override
  public void commitPendingDocOffsets() {
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
   * max.poll.interval.ms without polling, or once the document's partition was revoked or lost since it was polled. A
   * document that is not from Kafka, such as a child a stage created, has no partition and is always sendable. Called
   * from the Indexer's send threads.
   */
  boolean ownsInput(Document doc) {
    if (System.nanoTime() - lastReturnedPollStartNanos >= maxPollIntervalNanos) {
      return false;
    }
    if (!(doc instanceof KafkaDocument kafkaDoc)) {
      return true;
    }
    return offsetCommitTracker.isOutstanding(new TopicPartition(kafkaDoc.getTopic(), kafkaDoc.getPartition()),
        kafkaDoc.getOffset());
  }

  private void queueBatchResult(boolean sent, List<Document> batch) {
    List<PolledOffset> offsets = new ArrayList<>();
    for (Document doc : batch) {
      if (doc instanceof KafkaDocument kafkaDoc) {
        offsets.add(new PolledOffset(new TopicPartition(kafkaDoc.getTopic(), kafkaDoc.getPartition()), kafkaDoc.getOffset()));
      }
    }
    if (!offsets.isEmpty()) {
      batchResults.add(new BatchResult(sent, offsets));
    }
  }

  // Applies the Indexer's reports to the tracker. On the worker thread.
  private void drainBatchResults() {
    BatchResult result;
    while ((result = batchResults.poll()) != null) {
      for (PolledOffset polledOffset : result.offsets()) {
        if (result.sent()) {
          offsetCommitTracker.recordCompleted(polledOffset.partition(), polledOffset.offset());
        } else {
          offsetCommitTracker.recordNotSent(polledOffset.partition(), polledOffset.offset());
        }
      }
    }
  }

  // Removes the waiting documents of the given partitions from the queue: they were not indexed and their offsets are not
  // committed, so whoever polls the partition next indexes them. A document already taken by the Indexer is not
  // affected; its batch is skipped by the ownership check or, if already sent, indexed again by the next owner.
  private void purge(Collection<TopicPartition> partitions) {
    if (partitions.isEmpty()) {
      return;
    }
    pipelineDest.removeIf(doc -> doc instanceof KafkaDocument kafkaDoc
        && partitions.contains(new TopicPartition(kafkaDoc.getTopic(), kafkaDoc.getPartition())));
  }

  private record PolledOffset(TopicPartition partition, long offset) {
  }

  private record BatchResult(boolean sent, List<PolledOffset> offsets) {
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
   * Sends a processed document to the appropriate destination for documents waiting to be indexed.
   *
   */
  @Override
  public void sendForIndexing(Document document) throws MessengerException {
    try {
      pipelineDest.put(document);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new MessengerException("Interrupted while queueing document " + document.getId() + " for indexing", e);
    }
  }

  @Override
  public void sendFailed(Document document) {
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
  }

}

