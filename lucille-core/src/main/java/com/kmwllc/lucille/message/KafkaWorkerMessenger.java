package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.KafkaDocument;
import com.typesafe.config.Config;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * WorkerMessenger for a standalone Worker that reads its source topic and writes its dest topic through Kafka.
 *
 * <p>Processed documents are sent to the dest topic asynchronously, and source offsets are committed in batches:
 * once {@code worker.commitBatchSize} documents are uncommitted, or once the oldest uncommitted document is
 * {@code worker.commitBatchTimeout} milliseconds old, whichever comes first. Every commit first flushes the document
 * producer, so an offset is never committed before the dest-topic sends for the documents it covers have been
 * acknowledged; delivery is at-least-once. A send that the broker rejects is reported with a FAIL event for the
 * document whose send failed, before the commit. Pending offsets are also committed when partitions are revoked and
 * when the messenger is closed. These settings apply only to this messenger; local and hybrid workers ignore them.
 *
 * <p>Config parameters:
 * <ul>
 *   <li>worker.commitBatchSize (Integer, Optional) : Number of processed documents after which source offsets are
 *   committed. Defaults to {@value #DEFAULT_COMMIT_BATCH_SIZE}. Set to 1 to commit after every document.</li>
 *   <li>worker.commitBatchTimeout (Integer, Optional) : Milliseconds after which the oldest uncommitted document's
 *   offset is committed even if the batch is not full. Defaults to {@value #DEFAULT_COMMIT_BATCH_TIMEOUT}.</li>
 * </ul>
 */
public class KafkaWorkerMessenger implements WorkerMessenger {

  public static final int DEFAULT_COMMIT_BATCH_SIZE = 100;
  public static final int DEFAULT_COMMIT_BATCH_TIMEOUT = 100;

  private static final Logger log = LoggerFactory.getLogger(KafkaWorkerMessenger.class);
  private final Consumer<String, KafkaDocument> sourceConsumer;
  private final Producer<String, Document> kafkaDocumentProducer;
  private final Producer<String, String> kafkaEventProducer;
  private final Config config;
  private final Duration pollInterval;
  private final String pipelineName;
  private final DeserializationErrorHandler deserializationErrorHandler;
  private final int commitBatchSize;
  private final long commitBatchTimeoutNanos;

  // documents whose dest-topic send was rejected; added on the producer's I/O thread, drained on the worker thread
  private final Queue<Document> failedSends = new ConcurrentLinkedQueue<>();

  // touched only on the worker thread (rebalance callbacks run inside poll() on that thread)
  private int uncommittedCount = 0;
  private long oldestUncommittedNanos;

  public KafkaWorkerMessenger(Config config, String pipelineName) {
    // append random string to kafka client ID to prevent kafka from issuing a warning when multiple consumers
    // with the same client ID are started in separate worker threads
    this(config, pipelineName,
        KafkaUtils.createDocumentConsumer(config,
            "com.kmwllc.lucille-worker-" + pipelineName + "-" + RandomStringUtils.randomAlphanumeric(8)),
        KafkaUtils.createDocumentProducer(config),
        KafkaUtils.createEventProducer(config));
  }

  KafkaWorkerMessenger(Config config, String pipelineName, Consumer<String, KafkaDocument> sourceConsumer,
      Producer<String, Document> documentProducer, Producer<String, String> eventProducer) {
    this.config = config;
    this.pollInterval = KafkaUtils.getPollInterval(config);
    this.pipelineName = pipelineName;
    this.commitBatchSize = getPositiveInt(config, "worker.commitBatchSize", DEFAULT_COMMIT_BATCH_SIZE);
    this.commitBatchTimeoutNanos = Duration.ofMillis(
        getPositiveInt(config, "worker.commitBatchTimeout", DEFAULT_COMMIT_BATCH_TIMEOUT)).toNanos();
    this.kafkaDocumentProducer = documentProducer;
    this.kafkaEventProducer = eventProducer;
    this.sourceConsumer = sourceConsumer;
    this.sourceConsumer.subscribe(
        Collections.singletonList(KafkaUtils.getSourceTopicName(pipelineName, config)), new CommitOnRevoke());
    this.deserializationErrorHandler =
        new DeserializationErrorHandler(config, sourceConsumer);
  }

  private static int getPositiveInt(Config config, String path, int defaultValue) {
    int value = config.hasPath(path) ? config.getInt(path) : defaultValue;
    if (value < 1) {
      throw new IllegalArgumentException(path + " must be at least 1, but was " + value);
    }
    return value;
  }

  /**
   * Polls for a document that is waiting to be processed by the pipeline.
   *
   */
  @Override
  public Document pollDocToProcess() throws Exception {
    ConsumerRecords<String, KafkaDocument> consumerRecords;
    try {
      consumerRecords = sourceConsumer.poll(pollInterval);
    } catch (RecordDeserializationException e) {
      // rethrows unless configured to skip; the advanced position is committed with the next batch
      deserializationErrorHandler.handleOrRethrow(e);
      markUncommitted();
      return null;
    }
    KafkaUtils.validateAtMostOneRecord(consumerRecords);
    if (consumerRecords.count() > 0) {
      ConsumerRecord<String, KafkaDocument> record = consumerRecords.iterator().next();
      deserializationErrorHandler.onSuccessfulPoll(record);
      markUncommitted();
      KafkaDocument doc = record.value();
      doc.setKafkaMetadata(record);
      return doc;
    }
    return null;
  }

  private void markUncommitted() {
    if (uncommittedCount == 0) {
      oldestUncommittedNanos = System.nanoTime();
    }
    uncommittedCount++;
  }

  /**
   * Commits the source offsets of processed documents once the commit batch is full or has timed out; otherwise
   * does nothing. The Worker calls this after each document and after each empty poll, so an idle worker still
   * commits within roughly {@code worker.commitBatchTimeout} plus one poll interval.
   */
  @Override
  public void commitPendingDocOffsets() throws Exception {
    if (uncommittedCount == 0) {
      return;
    }
    if (uncommittedCount >= commitBatchSize || System.nanoTime() - oldestUncommittedNanos >= commitBatchTimeoutNanos) {
      flushAndCommit();
    }
  }

  /**
   * Waits for every dest-topic send to be acknowledged, reports rejected sends as FAIL events, and only then
   * commits the consumer's position. If the flush is interrupted, nothing is committed and the documents are
   * redelivered.
   */
  private void flushAndCommit() {
    if (uncommittedCount == 0) {
      return;
    }
    kafkaDocumentProducer.flush();
    reportFailedSends();
    sourceConsumer.commitSync();
    uncommittedCount = 0;
  }

  private void reportFailedSends() {
    Document document;
    while ((document = failedSends.poll()) != null) {
      try {
        sendEvent(document, "failed to send for indexing", Event.Type.FAIL);
      } catch (Exception e) {
        log.error("Error sending failure event for document: {}", document.getId(), e);
      }
    }
  }

  /**
   * Sends a processed document to the appropriate destination for documents waiting to be indexed. The send is
   * asynchronous; it is acknowledged (or reported as failed) before the document's source offset is committed.
   */
  @Override
  public void sendForIndexing(Document document) throws Exception {
    kafkaDocumentProducer.send(
        new ProducerRecord<>(KafkaUtils.getDestTopicName(pipelineName), document.getId(), document),
        (metadata, exception) -> {
          if (exception != null) {
            log.error("Failed to send document {} for indexing", document.getId(), exception);
            failedSends.add(document);
          }
        });
  }

  public void sendFailed(Document document) throws Exception {
    ProducerRecord<String, Document> producerRecord =
        new ProducerRecord<>(KafkaUtils.getFailTopicName(pipelineName), document.getId(), document);
    RecordMetadata metadata = kafkaDocumentProducer.send(producerRecord).get();
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
   * Sends an Event relating to a Document to the appropriate location for Events.
   *
   */
  @Override
  public void sendEvent(Event event) throws Exception {
    if (kafkaEventProducer == null) {
      return;
    }
    String confirmationTopicName = KafkaUtils.getEventTopicName(config, pipelineName, event.getRunId());
    RecordMetadata result = kafkaEventProducer.send(
        new ProducerRecord<>(confirmationTopicName, event.getDocumentId(), event.toString())).get();
  }

  /**
   * Commits any pending offsets (after their sends are acknowledged), then closes the consumer and producers.
   */
  @Override
  public void close() throws Exception {
    try {
      if (sourceConsumer != null && kafkaDocumentProducer != null) {
        flushAndCommit();
      }
    } catch (Exception e) {
      log.error("Error committing pending offsets on close", e);
    }
    if (sourceConsumer != null) {
      sourceConsumer.close();
    }
    if (kafkaDocumentProducer != null) {
      kafkaDocumentProducer.close();
    }
    if (kafkaEventProducer != null) {
      kafkaEventProducer.close();
    }
  }

  /**
   * Commits pending offsets before partitions move to another consumer, so the new owner does not reprocess
   * documents this worker already sent. Runs on the worker thread, inside poll() or close().
   */
  private class CommitOnRevoke implements ConsumerRebalanceListener {

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      try {
        flushAndCommit();
      } catch (Exception e) {
        // an exception thrown here would surface from poll() and stop the worker; the documents are redelivered
        log.error("Error committing pending offsets on partition revocation", e);
      }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
      // the partitions already belong to another consumer, so their offsets cannot be committed and they will be
      // redelivered there; the next commit covers only partitions still assigned to this consumer
    }
  }
}
