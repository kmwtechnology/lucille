package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.KafkaDocument;
import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

public class KafkaIndexerMessenger implements IndexerMessenger {

  private static final Logger log = LoggerFactory.getLogger(KafkaIndexerMessenger.class);
  private final Consumer<String, KafkaDocument> destConsumer;
  private final KafkaEventSender eventSender;
  private final String pipelineName;
  private final Config config;
  private final DeserializationErrorHandler deserializationErrorHandler;
  private final Duration pollInterval;

  public KafkaIndexerMessenger(Config config, String pipelineName) {
    this.pipelineName = pipelineName;
    String kafkaClientId = "com.kmwllc.lucille-indexer-" + pipelineName;
    this.destConsumer = KafkaUtils.createDocumentConsumer(config, kafkaClientId);
    this.destConsumer.subscribe(Collections.singletonList(KafkaUtils.getDestTopicName(pipelineName)));
    this.eventSender = new KafkaEventSender(config, pipelineName);
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
    ConsumerRecords<String, KafkaDocument> consumerRecords;
    try {
      consumerRecords = destConsumer.poll(pollInterval);
    } catch (RecordDeserializationException e) {
      // rethrows unless configured to skip; commit the position past the skipped record
      deserializationErrorHandler.handleOrRethrow(e);
      destConsumer.commitSync();
      return null;
    }
    KafkaUtils.validateAtMostOneRecord(consumerRecords);
    if (consumerRecords.count() > 0) {
      // offsets are committed synchronously to ensure that offsets are successfully committed and to reduce the likelihood of duplicate events being sent to the event topic.
      // This reduces the number of documents that might be reindexed in the event of an indexer crash/restart or in the case of a consumer group reblance.
      destConsumer.commitSync();
      ConsumerRecord<String, KafkaDocument> record = consumerRecords.iterator().next();
      deserializationErrorHandler.onSuccessfulPoll(record);
      KafkaDocument doc = record.value();
      doc.setKafkaMetadata(record);
      return doc;
    }
    return null;
  }

  @Override
  public void sendEvent(Document document, String message, Event.Type type) throws Exception {
    if (!eventSender.isEnabled()) {
      return;
    }
    sendEvent(new Event(document, message, type));
  }


  /**
   * Sends an Event relating to a Document to the appropriate location for Events, without waiting for it to be
   * acknowledged. It is delivered by the next sendEvents() or batchComplete() call.
   */
  @Override
  public void sendEvent(Event event) throws Exception {
    eventSender.send(event);
  }

  @Override
  public void sendEvents(List<Document> documents, String message, Event.Type type) throws Exception {
    if (!eventSender.isEnabled() || documents.isEmpty()) {
      return;
    }

    for (Document document : documents) {
      eventSender.send(new Event(document, message, type));
    }
    eventSender.flush();
  }

  @Override
  public void close() throws Exception {
    try {
      destConsumer.close();
    } finally {
      eventSender.close();
    }
  }

  /**
   * Delivers the batch's events (FAIL events are sent one at a time) before the Indexer moves on.
   */
  @Override
  public void batchComplete(List<Document> batch) throws Exception {
    eventSender.flush();
  }

}
