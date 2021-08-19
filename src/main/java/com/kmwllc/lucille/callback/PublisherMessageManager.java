package com.kmwllc.lucille.callback;

import com.kmwllc.lucille.core.Document;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public interface PublisherMessageManager {

  public void sendForProcessing(Document document) throws Exception;

  public void close();
}
