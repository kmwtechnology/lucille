package com.kmwllc.lucille.callback;

import com.kmwllc.lucille.core.ConfigAccessor;
import com.kmwllc.lucille.core.Document;
import com.typesafe.config.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Publisher {

  public static final Logger log = LoggerFactory.getLogger(Publisher.class);

  private final Config config = ConfigAccessor.loadConfig();

  private final KafkaProducer<String, String> kafkaProducer;

  private final String runId;

  private int numPublished = 0;

  // TODO: consider changing these to ConcurrentHashSets, but also consider how we should handle duplicate doc IDs
  private List<String> docIdsToTrack = Collections.synchronizedList(new ArrayList<String>());
  private List<String> docIdsIndexedBeforeTracking = Collections.synchronizedList(new ArrayList<String>());


  public Publisher(String runId) {
    this.runId = runId;
    this.kafkaProducer = KafkaUtils.createProducer();
  }

  public void publish(Document document) throws Exception {
    document.setField("run_id", runId);
    RecordMetadata result = (RecordMetadata) kafkaProducer.send(
      new ProducerRecord(config.getString("kafka.sourceTopic"), document.getId(), document.toString())).get();
    log.info("Published: " + result);
    kafkaProducer.flush();
    docIdsToTrack.add(document.getId());
    numPublished++;
  }

  public int numPublished() {
    return numPublished;
  }

  public void close() throws Exception {
    kafkaProducer.close();
  }

  public void handleEvent(Event event) {
    String docId = event.getDocumentId();

    if (event.isCreate()) {
      if (!docIdsIndexedBeforeTracking.remove(docId)) {
        docIdsToTrack.add(docId);
      }
    } else {
      if (!docIdsToTrack.remove(docId)) {
        docIdsIndexedBeforeTracking.add(docId);
      }
    }
  }

  public boolean hasReceivedAllExepectedEvents() {
    return docIdsToTrack.isEmpty() && docIdsIndexedBeforeTracking.isEmpty();
  }

  public int countExpectedIndexEvents() {
    return docIdsToTrack.size();
  }

  public int countExpectedCreateEvents() {
    return docIdsIndexedBeforeTracking.size();
  }

}
