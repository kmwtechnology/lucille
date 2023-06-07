package com.kmwllc.lucille.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.KafkaDocument;
import com.kmwllc.lucille.util.LinkedMultiMap;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class KafkaDocumentDeserializer implements Deserializer<Document> {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public Document deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    }

    try {
      return new KafkaDocument(KafkaDocument.MAPPER_WITH_TYPING.readValue(data, LinkedMultiMap.class));
    } catch (Exception e) {
      throw new SerializationException("Error deserializing document", e);
    }
  }
}
