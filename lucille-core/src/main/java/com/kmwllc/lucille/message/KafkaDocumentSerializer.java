package com.kmwllc.lucille.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.Document;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class KafkaDocumentSerializer implements Serializer<Document> {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public byte[] serialize(String topic, Document doc) {
    if (doc == null) {
      return null;
    }

    try {
      return MAPPER.writeValueAsBytes(doc);
    } catch (Exception e) {
      throw new SerializationException("Error serializing document", e);
    }
  }
}
