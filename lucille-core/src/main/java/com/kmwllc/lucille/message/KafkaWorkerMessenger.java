package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.KafkaDocument;
import com.typesafe.config.Config;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class KafkaWorkerMessenger implements WorkerMessenger {

  private static final Logger log = LoggerFactory.getLogger(KafkaWorkerMessenger.class);
  private final Consumer<String, KafkaDocument> sourceConsumer;
  private final KafkaProducer<String, Document> kafkaDocumentProducer;
  private final KafkaProducer<String, String> kafkaEventProducer;
  private final Config config;
  private final String pipelineName;

  public KafkaWorkerMessenger(Config config, String pipelineName) {
    this.config = config;
    this.pipelineName = pipelineName;
    this.kafkaDocumentProducer = KafkaUtils.createDocumentProducer(config);
    this.kafkaEventProducer = KafkaUtils.createEventProducer(config);
    // append random string to kafka client ID to prevent kafka from issuing a warning when multiple consumers
    // with the same client ID are started in separate worker threads
    String kafkaClientId = "com.kmwllc.lucille-worker-" + pipelineName + "-" + RandomStringUtils.randomAlphanumeric(8);
    this.sourceConsumer = KafkaUtils.createDocumentConsumer(config, kafkaClientId);
    this.sourceConsumer.subscribe(Collections.singletonList(KafkaUtils.getSourceTopicName(pipelineName, config)));
  }

  /**
   * Polls for a document that is waiting to be processed by the pipeline.
   *
   */
  @Override
  public Document pollDocToProcess() throws Exception {
    ConsumerRecords<String, KafkaDocument> consumerRecords = sourceConsumer.poll(KafkaUtils.POLL_INTERVAL);
    KafkaUtils.validateAtMostOneRecord(consumerRecords);
    if (consumerRecords.count() > 0) {
      ConsumerRecord<String, KafkaDocument> record = consumerRecords.iterator().next();
      KafkaDocument doc = record.value();
      // TODO: Appropriate place to set the MDC.

      doc.setKafkaMetadata(record);
      return doc;
    }
    return null;
  }

  @Override
  public void commitPendingDocOffsets() throws Exception {
    sourceConsumer.commitSync();
  }

  /**
   * Sends a processed document to the appropriate destination for documents waiting to be indexed.
   *
   */
  @Override
  public void sendForIndexing(Document document) throws Exception {
    RecordMetadata result = kafkaDocumentProducer.send(
        new ProducerRecord<>(KafkaUtils.getDestTopicName(pipelineName), document.getId(), document)).get();
    kafkaDocumentProducer.flush();
  }

  public void sendFailed(Document document) throws Exception {
    ProducerRecord<String, Document> producerRecord =
        new ProducerRecord<>(KafkaUtils.getFailTopicName(pipelineName), document.getId(), document);
    RecordMetadata metadata = kafkaDocumentProducer.send(producerRecord).get();
    kafkaDocumentProducer.flush();
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
    kafkaEventProducer.flush();
  }

  @Override
  public void close() throws Exception {
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

}
