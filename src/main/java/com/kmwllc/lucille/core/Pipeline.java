package com.kmwllc.lucille.core;

import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

/**
 * A sequence of processing Stages to be applied to incoming Documents.
 */
public class Pipeline {

  public static Pipeline fromConfig(Config config) {
    return null;
  }

  public List<Document> processDocument(Document document) throws StageException {
    return null;
  }

  public List<Document> processKafkaMessage(ConsumerRecord<String,String> record) throws Exception {
    // TODO: parse message into a Document and delegate to processDocument
    return null;
  }

}
