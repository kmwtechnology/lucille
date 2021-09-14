package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Event;
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

public class KafkaWorkerMessageManager implements WorkerMessageManager {

  public static final Logger log = LoggerFactory.getLogger(KafkaWorkerMessageManager.class);
  private final Consumer<String, String> sourceConsumer;
  private final KafkaProducer<String, String> kafkaProducer;
  private final Config config;
  private final String pipelineName;

  public KafkaWorkerMessageManager(Config config, String pipelineName) {
    this.config = config;
    this.pipelineName = pipelineName;
    this.kafkaProducer = KafkaUtils.createProducer(config);
    Properties consumerProps = KafkaUtils.createConsumerProps(config);
    consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "lucille-worker-" + pipelineName);
    this.sourceConsumer = new KafkaConsumer(consumerProps);
    this.sourceConsumer.subscribe(Collections.singletonList(KafkaUtils.getSourceTopicName(pipelineName)));
  }

  /**
   * Polls for a document that is waiting to be processed by the pipeline.
   *
   */
  @Override
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
  @Override
  public void sendCompleted(Document document) throws Exception {
    RecordMetadata result = (RecordMetadata) kafkaProducer.send(
      new ProducerRecord(KafkaUtils.getDestTopicName(pipelineName), document.getId(), document.toString())).get();
    kafkaProducer.flush();
  }

  /**
   * Sends an Event relating to a Document to the appropriate location for Events.
   *
   */
  @Override
  public void sendEvent(Event event) throws Exception {
    String confirmationTopicName = KafkaUtils.getEventTopicName(pipelineName, event.getRunId());
    RecordMetadata result = (RecordMetadata)  kafkaProducer.send(
      new ProducerRecord(confirmationTopicName, event.getDocumentId(), event.toString())).get();
    kafkaProducer.flush();
  }

  @Override
  public void close() throws Exception {
    sourceConsumer.close();
    kafkaProducer.close();
  }

}
