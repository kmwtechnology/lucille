package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.KafkaDocument;
import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaIndexerMessenger implements IndexerMessenger {

  private static final Logger log = LoggerFactory.getLogger(KafkaIndexerMessenger.class);
  private final Consumer<String, KafkaDocument> destConsumer;
  private final KafkaProducer<String, Document> kafkaDocumentProducer;
  private final KafkaProducer<String, String> kafkaEventProducer;
  private final String pipelineName;
  private final Config config;

  // Tracks destination-topic offsets and commits them on batch completion (at-least-once). See OffsetCommitTracker.
  private final OffsetCommitTracker offsetCommitTracker;

  private final DeserializationErrorHandler deserializationErrorHandler;
  private final Duration pollInterval;

  public KafkaIndexerMessenger(Config config, String pipelineName) {
    this(config, pipelineName, KafkaUtils.createDocumentConsumer(config, "com.kmwllc.lucille-indexer-" + pipelineName));
  }

  // For tests: allows a MockConsumer (or other test double) to be supplied in place of the real Kafka consumer.
  KafkaIndexerMessenger(Config config, String pipelineName, Consumer<String, KafkaDocument> destConsumer) {
    this.pipelineName = pipelineName;
    this.destConsumer = destConsumer;
    this.offsetCommitTracker = new OffsetCommitTracker(destConsumer);
    this.destConsumer.subscribe(Collections.singletonList(KafkaUtils.getDestTopicName(pipelineName)),
        offsetCommitTracker.rebalanceListener());
    // The document producer is used only to write to the fail topic (sendFailed). Create it only when the fail topic
    // is enabled; otherwise an Indexer not using the fail topic would need Kafka producer config it has no reason to
    // supply. Mirrors how createEventProducer returns null when events are disabled.
    this.kafkaDocumentProducer = KafkaUtils.indexerFailTopicEnabled(config)
        ? KafkaUtils.createDocumentProducer(config) : null;
    this.kafkaEventProducer = KafkaUtils.createEventProducer(config);
    this.config = config;
    this.deserializationErrorHandler =
        new DeserializationErrorHandler(config, destConsumer);
    this.pollInterval = KafkaUtils.getPollInterval(config);
  }

  /**
   * Polls for a document that has been processed by the pipeine and is waiting to be indexed.
   */
  @Override
  public Document pollDocToIndex() throws Exception {
    // This is the only place the consumer is polled during a run, so it is the only place (besides close()) a rebalance
    // callback, and with it the pre-revocation hook, can run. The Indexer calls this from its run loop, never while
    // dispatching or draining a batch, so the hook never re-enters the dispatcher.
    ConsumerRecords<String, KafkaDocument> consumerRecords;
    try {
      consumerRecords = destConsumer.poll(pollInterval);
    } catch (RecordDeserializationException e) {
      // Rethrows unless configured to skip. Nothing is committed here: committing the consumer's position would commit
      // past documents still in the open batch or in flight. The skipped offset is never recorded as polled, so the
      // commit frontier moves past it once the surrounding documents complete; until then, a restart re-reads (and
      // skips) it again.
      deserializationErrorHandler.handleOrRethrow(e);
      return null;
    }
    KafkaUtils.validateAtMostOneRecord(consumerRecords);
    if (consumerRecords.count() > 0) {
      // Offsets are not committed here. Committing at poll, before the document is indexed, is at-most-once: a crash
      // after the commit but before indexing would skip the document entirely. Instead the offset is committed in
      // batchComplete, once the document has been indexed and its events emitted (at-least-once).
      ConsumerRecord<String, KafkaDocument> record = consumerRecords.iterator().next();
      deserializationErrorHandler.onSuccessfulPoll(record);
      KafkaDocument doc = record.value();
      doc.setKafkaMetadata(record);
      // Record the polled offset, so the commit frontier can advance across it once its batch completes.
      offsetCommitTracker.recordPolled(new TopicPartition(record.topic(), record.partition()), record.offset());
      return doc;
    }
    return null;
  }

  // Commits offsets on batch completion (not at poll), so it is safe for concurrent batches.
  @Override
  public boolean commitsOnBatchCompletion() {
    return true;
  }

  // Registers the Indexer's flush-and-drain action, run from the OffsetCommitTracker's revocation callback (on the
  // indexer thread, inside the poll() in pollDocToIndex or close()) before offsets are committed and the partition is
  // handed off.
  @Override
  public void setPreRevocationHook(Runnable hook) {
    offsetCommitTracker.setPreRevocationHook(hook);
  }

  // The destination-topic partitions currently assigned to this consumer. Package-private for tests that drive real
  // rebalances and need to observe when the assignment has settled.
  Set<TopicPartition> assignment() {
    return destConsumer.assignment();
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
    RecordMetadata result = (RecordMetadata) kafkaEventProducer.send(
        new ProducerRecord(confirmationTopicName, event.getDocumentId(), event.toString())).get();
  }

  @Override
  public void sendEvents(List<Document> documents, String message, Event.Type type) throws Exception {
    if (kafkaEventProducer == null || documents.isEmpty()) {
      return;
    }

    AtomicReference<Exception> sendException = new AtomicReference<>();

    for (Document document : documents) {
      Event event = new Event(document, message, type);
      String confirmationTopicName = KafkaUtils.getEventTopicName(config, pipelineName, event.getRunId());
      kafkaEventProducer.send(
          new ProducerRecord<>(confirmationTopicName, event.getDocumentId(), event.toString()),
          (metadata, exception) -> {
            if (exception != null) {
              log.error("Failed to send event for doc: {}", event.getDocumentId(), exception);
              sendException.compareAndSet(null, exception);
            }
          });
    }

    kafkaEventProducer.flush();

    Exception e = sendException.get();
    if (e != null) {
      throw new Exception("Failed to send one or more events", e);
    }
  }

  @Override
  public void close() throws Exception {
    // Commit offsets of batches completed but not yet committed, so a clean shutdown does not force their re-delivery.
    offsetCommitTracker.commitOnClose();
    destConsumer.close();
    if (kafkaDocumentProducer != null) {
      kafkaDocumentProducer.close();
    }
  }

  @Override
  public void sendFailed(Document document) throws Exception {
    if (kafkaDocumentProducer == null) {
      // Should not happen: the Indexer only calls sendFailed when indexer.sendFailedDocsToTopic is enabled, which is
      // exactly when the producer is created. Fail loudly on a misconfiguration rather than NPE.
      throw new IllegalStateException("sendFailed called but indexer.sendFailedDocsToTopic is not enabled");
    }
    ProducerRecord<String, Document> producerRecord =
        new ProducerRecord<>(KafkaUtils.getFailTopicName(pipelineName), document.getId(), document);
    // send(...).get() blocks until the broker acks, so the fail-topic write is durable before we return.
    kafkaDocumentProducer.send(producerRecord).get();
  }

  /**
   * Commits the destination-topic offsets of a completed batch, on batch completion rather than at poll (at-least-once
   * delivery). The offset bookkeeping, including rebalance safety and failed-commit retry, lives in
   * {@link OffsetCommitTracker}.
   */
  @Override
  public void batchComplete(List<Document> batch) throws Exception {
    if (batch.isEmpty()) {
      return;
    }
    offsetCommitTracker.batchCompleted(batch);
  }

}
