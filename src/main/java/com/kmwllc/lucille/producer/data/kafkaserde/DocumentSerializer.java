package com.kmwllc.lucille.producer.data.kafkaserde;

import com.kmwllc.lucille.core.Document;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class DocumentSerializer implements Serializer<Document> {

  @Override
  public byte[] serialize(String topic, Document data) {
    return data.toString().getBytes(StandardCharsets.UTF_8);
  }
}
