package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.HashMapDocument;
import com.kmwllc.lucille.core.KafkaDocument;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class KafkaDocumentSerializer implements Serializer<Document> {

  @Override
  public byte[] serialize(String topic, Document doc) {
    if (doc == null) {
      return null;
    }

    try {
      return KafkaDocument.MAPPER_WITH_TYPING.writeValueAsBytes(((HashMapDocument)doc).getData());
    } catch (Exception e) {
      throw new SerializationException("Error serializing document", e);
    }
  }
}
