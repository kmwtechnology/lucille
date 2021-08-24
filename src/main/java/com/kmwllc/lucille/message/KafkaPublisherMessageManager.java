package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.ConfigAccessor;
import com.kmwllc.lucille.core.Document;
import com.typesafe.config.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaPublisherMessageManager implements PublisherMessageManager {

  private final Config config = ConfigAccessor.loadConfig();

  private final KafkaProducer<String, String> kafkaProducer;

  public KafkaPublisherMessageManager() {
    this.kafkaProducer = KafkaUtils.createProducer();
  }

  public void sendForProcessing(Document document) throws Exception {
    RecordMetadata result = (RecordMetadata) kafkaProducer.send(
      new ProducerRecord(config.getString("kafka.sourceTopic"), document.getId(), document.toString())).get();
    kafkaProducer.flush();
  }

  public void close() {
    kafkaProducer.close();
  }

}
