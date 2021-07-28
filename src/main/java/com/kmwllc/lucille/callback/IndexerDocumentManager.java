package com.kmwllc.lucille.callback;

import com.kmwllc.lucille.core.ConfigAccessor;
import com.kmwllc.lucille.core.Document;
import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class IndexerDocumentManager {

  public static final Logger log = LoggerFactory.getLogger(IndexerDocumentManager.class);
  private final Config config = ConfigAccessor.loadConfig();
  private final SolrClient solrClient;
  private final Consumer<String, String> destConsumer;
  private final KafkaProducer<String, String> kafkaProducer;

  //TODO
  private static final String SOLR_URL = "http://localhost:8983/solr/callbacktest";

  public IndexerDocumentManager() {
    this.solrClient = new HttpSolrClient.Builder(SOLR_URL).build();
    Properties consumerProps = KafkaUtils.getConsumerProps();
    consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "lucille-2");
    this.destConsumer = new KafkaConsumer(consumerProps);
    this.destConsumer.subscribe(Collections.singletonList(config.getString("kafka.destTopic")));

    this.kafkaProducer = KafkaUtils.getProducer();
  }

  public Document retrieveCompleted() throws Exception {
    for (int i=0; i<5; i++) {
      ConsumerRecords<String, String> consumerRecords = destConsumer.poll(KafkaUtils.POLL_INTERVAL);
      if (consumerRecords.count() > 0) {
        destConsumer.commitSync();
        System.out.println("INDEXER: FOUND RECORD");
        ConsumerRecord<String, String> record = consumerRecords.iterator().next();
        return Document.fromJsonString(record.value());
      }
    }
    return null;
  }

  public void sendToSolr(List<Document> documents) throws Exception {
    List<SolrInputDocument> solrDocs = new ArrayList();
    for (Document doc : documents) {
      Map<String,Object> map = doc.asMap();
      SolrInputDocument solrDoc = new SolrInputDocument();
      for (String key : map.keySet()) {
        Object value = map.get(key);
        solrDoc.setField(key,value);
      }
      solrDocs.add(solrDoc);
    }
    solrClient.add(solrDocs);
  }

  public void submitReceipt(Receipt receipt) throws Exception {
    String receiptTopicName = getReceiptTopicName(receipt.getRunId());
    RecordMetadata result = (RecordMetadata)  kafkaProducer.send(
      new ProducerRecord(receiptTopicName, receipt.getDocumentId(), receipt.toString())).get();
    log.info("SUBMIT RECEIPT: " + result);
    kafkaProducer.flush();
  }

  public String getReceiptTopicName(String runId) {
    return config.getString("kafka.receiptTopic") + "_" + runId;
  }

  public void close() throws Exception {
    destConsumer.close();
  }

}
