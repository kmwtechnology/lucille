package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class KafkaPublisherMessageManager implements PublisherMessageManager {

  public static final Logger log = LoggerFactory.getLogger(KafkaPublisherMessageManager.class);

  private final Config config;
  private KafkaProducer<String, Document> kafkaProducer;
  private Consumer<String, String> eventConsumer;
  private String runId;
  private String pipelineName;

  public KafkaPublisherMessageManager(Config config) {
    this.config = config;
  }

  public void initialize(String runId, String pipelineName) throws Exception {
    if (this.runId != null) {
      throw new Exception("Already initialized.");
    }
    this.runId = runId;
    this.pipelineName = pipelineName;

    // create event topic explicitly with one partition
    KafkaUtils.createEventTopic(config, pipelineName, runId);

    String kafkaClientId = "com.kmwllc.lucille-publisher-" + pipelineName;
    String eventTopicName = KafkaUtils.getEventTopicName(pipelineName, runId);
    this.eventConsumer = KafkaUtils.createEventConsumer(config, kafkaClientId);
    this.eventConsumer.subscribe(Collections.singletonList(eventTopicName));
    this.kafkaProducer = KafkaUtils.createDocumentProducer(config);
  }

  @Override
  public String getRunId() {
    return runId;
  }

  public void sendForProcessing(Document document) throws Exception {
    RecordMetadata result = (RecordMetadata) kafkaProducer.send(
        new ProducerRecord(KafkaUtils.getSourceTopicName(pipelineName, config), document.getId(), document)).get();
    kafkaProducer.flush();
  }

  /**
   * Polls for an Event that is waiting to be consumed.
   */
  @Override
  public Event pollEvent() throws Exception {
    ConsumerRecords<String, String> consumerRecords = eventConsumer.poll(KafkaUtils.POLL_INTERVAL);
    KafkaUtils.validateAtMostOneRecord(consumerRecords);
    if (consumerRecords.count() > 0) {
      eventConsumer.commitSync();
      ConsumerRecord<String, String> record = consumerRecords.iterator().next();
      return Event.fromJsonString(record.value());
    }
    return null;
  }

  public void close() {
    if (kafkaProducer != null) {
      try {
        kafkaProducer.close();
      } catch (Exception e) {
        log.error("Couldn't close kafka producer", e);
      }
    }
    if (eventConsumer != null) {
      try {
        eventConsumer.close();
      } catch (Exception e) {
        log.error("Couldn't close kafka event consumer", e);
      }
    }
  }

}
