package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.KafkaDocument;
import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaIndexerMessenger implements IndexerMessenger {

  private static final Logger log = LoggerFactory.getLogger(KafkaIndexerMessenger.class);
  private final Consumer<String, KafkaDocument> destConsumer;
  private final KafkaProducer<String, String> kafkaEventProducer;
  private final String pipelineName;
  private final Config config;

  public KafkaIndexerMessenger(Config config, String pipelineName) {
    this(config, pipelineName, KafkaUtils.createDocumentConsumer(config, "com.kmwllc.lucille-indexer-" + pipelineName));
  }

  // For tests: allows a MockConsumer (or other test double) to be supplied in place of the real Kafka consumer.
  KafkaIndexerMessenger(Config config, String pipelineName, Consumer<String, KafkaDocument> destConsumer) {
    this.pipelineName = pipelineName;
    this.destConsumer = destConsumer;
    this.destConsumer.subscribe(Collections.singletonList(KafkaUtils.getDestTopicName(pipelineName)));
    this.kafkaEventProducer = KafkaUtils.createEventProducer(config);
    this.config = config;
  }

  /**
   * Polls for a document that has been processed by the pipeine and is waiting to be indexed.
   */
  @Override
  public Document pollDocToIndex() throws Exception {
    ConsumerRecords<String, KafkaDocument> consumerRecords = destConsumer.poll(KafkaUtils.POLL_INTERVAL);
    KafkaUtils.validateAtMostOneRecord(consumerRecords);
    if (consumerRecords.count() > 0) {
      // Offsets are not committed here. Committing at poll, before the document is indexed, is at-most-once: a crash
      // after the commit but before indexing would skip the document entirely. Instead the offset is committed in
      // batchComplete, once the document has been indexed and its events emitted (at-least-once).
      ConsumerRecord<String, KafkaDocument> record = consumerRecords.iterator().next();
      KafkaDocument doc = record.value();
      doc.setKafkaMetadata(record);
      return doc;
    }
    return null;
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
    destConsumer.close();
  }

  /**
   * Commits the destination-topic offsets of a completed batch. For each partition represented in the batch, commits
   * the offset after that partition's last record in the batch (the next offset to read). Because the Indexer completes
   * batches in the order it dispatched them, every record below a committed offset has been indexed, so a committed
   * offset is safe: a crash re-reads from it and loses nothing. Delivery is at-least-once — records polled but not yet
   * committed are re-delivered (and re-indexed, idempotently) after a crash or rebalance.
   */
  @Override
  public void batchComplete(List<Document> batch) throws Exception {
    if (batch.isEmpty()) {
      return;
    }
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    for (Document doc : batch) {
      if (!(doc instanceof KafkaDocument)) {
        continue;
      }
      KafkaDocument kafkaDoc = (KafkaDocument) doc;
      TopicPartition partition = new TopicPartition(kafkaDoc.getTopic(), kafkaDoc.getPartition());
      // The committed offset is the offset of the next record to read, so add one to the last record processed.
      long nextOffset = kafkaDoc.getOffset() + 1;
      OffsetAndMetadata current = offsets.get(partition);
      if (current == null || current.offset() < nextOffset) {
        offsets.put(partition, new OffsetAndMetadata(nextOffset));
      }
    }
    if (!offsets.isEmpty()) {
      destConsumer.commitSync(offsets);
    }
  }

}
