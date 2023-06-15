package com.kmwllc.lucille.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.Document;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class KafkaDocumentSerializer implements Serializer<Document> {

  @Override
  public byte[] serialize(String topic, Document doc) {
    if (doc == null) {
      return null;
    }

    try {
      return doc.writeAsBytes();
    } catch (Exception e) {
      throw new SerializationException("Error serializing document", e);
    }
  }
}
