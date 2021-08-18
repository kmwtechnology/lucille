package com.kmwllc.lucille.callback;

import com.kmwllc.lucille.core.ConfigAccessor;
import com.kmwllc.lucille.core.Document;
import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

public class WorkerDocumentManager {

  public static final Logger log = LoggerFactory.getLogger(WorkerDocumentManager.class);
  private final Config config = ConfigAccessor.loadConfig();
  private final Consumer<String, String> sourceConsumer;
  private final KafkaProducer<String, String> kafkaProducer;

  public WorkerDocumentManager() {
    this.kafkaProducer = KafkaUtils.createProducer();
    Properties consumerProps = KafkaUtils.createConsumerProps();
    consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "lucille-1");
    this.sourceConsumer = new KafkaConsumer(consumerProps);
    this.sourceConsumer.subscribe(Collections.singletonList(config.getString("kafka.sourceTopic")));
  }

  /**
   * Polls for a document that is waiting to be processed by the pipeline.
   *
   */
  public Document pollDocToProcess() throws Exception {
    ConsumerRecords<String, String> consumerRecords = sourceConsumer.poll(KafkaUtils.POLL_INTERVAL);
    if (consumerRecords.count() > 0) {
      sourceConsumer.commitSync();
      ConsumerRecord<String, String> record = consumerRecords.iterator().next();
      return Document.fromJsonString(record.value());
    }
    return null;
  }

  /**
   * Sends a processed document to the appropriate destination for documents waiting to be indexed.
   *
   */
  public void sendCompleted(Document document) throws Exception {
    RecordMetadata result = (RecordMetadata) kafkaProducer.send(
      new ProducerRecord(config.getString("kafka.destTopic"), document.getId(), document.toString())).get();
    kafkaProducer.flush();
  }

  /**
   * Sends an Event relating to a Document to the appropriate location for Events.
   *
   */
  public void sendEvent(Event event) throws Exception {
    String confirmationTopicName = KafkaUtils.getEventTopicName(event.getRunId());
    RecordMetadata result = (RecordMetadata)  kafkaProducer.send(
      new ProducerRecord(confirmationTopicName, event.getDocumentId(), event.toString())).get();
    kafkaProducer.flush();
  }

  public void close() throws Exception {
    sourceConsumer.close();
    kafkaProducer.close();
  }

}
