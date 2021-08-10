package com.kmwllc.lucille.callback;

import com.kmwllc.lucille.core.ConfigAccessor;
import com.kmwllc.lucille.core.Document;
import com.typesafe.config.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDocumentManager {

  public static final Logger log = LoggerFactory.getLogger(ProducerDocumentManager.class);

  private final Config config = ConfigAccessor.loadConfig();

  private final KafkaProducer<String, String> kafkaProducer;

  public ProducerDocumentManager() {
    this.kafkaProducer = KafkaUtils.createProducer();
  }

  public void submitForProcessing(Document document) throws Exception {
    RecordMetadata result = (RecordMetadata) kafkaProducer.send(
      new ProducerRecord(config.getString("kafka.sourceTopic"), document.getId(), document.toString())).get();
    log.info("SUBMIT FOR PROCESSING: " + result);
    kafkaProducer.flush();
  }

  public void close() throws Exception {
    kafkaProducer.close();
  }


}
