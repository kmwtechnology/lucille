package com.kmwllc.lucille.producer.data.kafkaserde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.DocumentException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;

public class DocumentDeserializer implements Deserializer<Document> {
  private static final Logger log = LogManager.getLogger(DocumentDeserializer.class);

  @Override
  public Document deserialize(String topic, byte[] data) {
    try {
      return Document.fromJsonString(new String(data, StandardCharsets.UTF_8));
    } catch (DocumentException | JsonProcessingException e) {
      log.error("Could not deserialize Document", e);
      return null;
    }
  }
}
