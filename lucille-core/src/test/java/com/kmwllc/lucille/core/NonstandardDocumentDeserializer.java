package com.kmwllc.lucille.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;


/**
 * Deserializes json containing "myId" into KafkaDocuments which require "id"
 * For testing purposes only
 */
public class NonstandardDocumentDeserializer implements Deserializer<Document> {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public Document deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    }

    try {
      ObjectNode node = (ObjectNode) MAPPER.readTree(data);
      if (!node.has("id") && node.has("myId")) {
        node.set("id", node.get("myId"));
      }
      return new KafkaDocument(node);
    } catch (Exception e) {
      throw new SerializationException("Error deserializing document", e);
    }
  }

}