package com.kmwllc.lucille.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.KafkaDocument;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class KafkaDocumentDeserializer implements Deserializer<JsonDocument> {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public JsonDocument deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    }

    try {
      return new KafkaDocument((ObjectNode) MAPPER.readTree(data));
    } catch (Exception e) {
      throw new SerializationException("Error deserializing document", e);
    }
  }
}
