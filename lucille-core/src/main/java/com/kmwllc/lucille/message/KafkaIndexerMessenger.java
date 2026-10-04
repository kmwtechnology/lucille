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
  private final KafkaProducer<String, String> kafkaEventProducer;
  private final String pipelineName;
  private final Config config;
  // Tracks destination-topic offsets and commits them on batch completion (at-least-once). See OffsetCommitTracker.
  private final OffsetCommitTracker offsetCommitTracker;

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
    this.kafkaEventProducer = KafkaUtils.createEventProducer(config);
    this.config = config;
  }

  /**
   * Polls for a document that has been processed by the pipeine and is waiting to be indexed.
   */
  @Override
  public Document pollDocToIndex() throws Exception {
    // Undo any pause applied by keepAlive() so this poll can deliver records again. paused() reports only partitions
    // still assigned, so partitions revoked since the pause are simply dropped from the set.
    Set<TopicPartition> paused = destConsumer.paused();
    if (!paused.isEmpty()) {
      destConsumer.resume(paused);
    }

    ConsumerRecords<String, KafkaDocument> consumerRecords = destConsumer.poll(KafkaUtils.POLL_INTERVAL);
    KafkaUtils.validateAtMostOneRecord(consumerRecords);
    if (consumerRecords.count() > 0) {
      // Offsets are not committed here. Committing at poll, before the document is indexed, is at-most-once: a crash
      // after the commit but before indexing would skip the document entirely. Instead the offset is committed in
      // batchComplete, once the document has been indexed and its events emitted (at-least-once).
      ConsumerRecord<String, KafkaDocument> record = consumerRecords.iterator().next();
      // Record how far we have polled this partition in the current assignment, so a later rebalance can tell which
      // completed batches hold records from this assignment (safe to commit) versus an earlier one (stale).
      offsetCommitTracker.recordPolled(new TopicPartition(record.topic(), record.partition()), record.offset() + 1);
      KafkaDocument doc = record.value();
      doc.setKafkaMetadata(record);
      return doc;
    }
    return null;
  }

  /**
   * Polls the consumer so it stays in its consumer group during a long wait, without delivering any records or moving
   * the position {@link #pollDocToIndex()} continues from. All currently assigned partitions are paused first, so the
   * poll returns nothing from them; {@link #pollDocToIndex()} resumes them on its next call. A partition newly assigned
   * during this poll is not yet paused and may return a record, so the consumer seeks back to the first record returned
   * for each such partition, leaving it to be delivered by a later {@link #pollDocToIndex()}.
   */
  // Commits offsets on batch completion (not at poll), so it is safe for concurrent batches.
  @Override
  public boolean supportsConcurrentBatches() {
    return true;
  }

  @Override
  public void keepAlive() throws Exception {
    destConsumer.pause(destConsumer.assignment());
    ConsumerRecords<String, KafkaDocument> consumerRecords = destConsumer.poll(Duration.ZERO);
    for (TopicPartition partition : consumerRecords.partitions()) {
      List<ConsumerRecord<String, KafkaDocument>> records = consumerRecords.records(partition);
      if (!records.isEmpty()) {
        destConsumer.seek(partition, records.get(0).offset());
      }
    }
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
