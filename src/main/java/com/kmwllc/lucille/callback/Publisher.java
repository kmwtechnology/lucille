package com.kmwllc.lucille.callback;

import com.kmwllc.lucille.core.ConfigAccessor;
import com.kmwllc.lucille.core.Document;
import com.typesafe.config.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Publisher {

  public static final Logger log = LoggerFactory.getLogger(Publisher.class);

  private final Config config = ConfigAccessor.loadConfig();

  private final KafkaProducer<String, String> kafkaProducer;

  private final String runId;

  private final List<String> documentIds;

  private int numPublished = 0;

  public Publisher(String runId, List<String> documentIds) {
    this.runId = runId;
    this.documentIds = documentIds;
    this.kafkaProducer = KafkaUtils.createProducer();
  }

  public void publish(Document document) throws Exception {
    document.setField("run_id", runId);
    RecordMetadata result = (RecordMetadata) kafkaProducer.send(
      new ProducerRecord(config.getString("kafka.sourceTopic"), document.getId(), document.toString())).get();
    log.info("Published: " + result);
    kafkaProducer.flush();
    documentIds.add(document.getId());
    numPublished++;
  }

  public int numPublished() {
    return numPublished;
  }

  public void close() throws Exception {
    kafkaProducer.close();
  }
}
