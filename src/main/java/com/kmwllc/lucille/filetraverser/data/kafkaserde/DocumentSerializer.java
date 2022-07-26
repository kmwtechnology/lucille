package com.kmwllc.lucille.filetraverser.data.kafkaserde;

import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.JsonDocument;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class DocumentSerializer implements Serializer<JsonDocument> {

  @Override
  public byte[] serialize(String topic, JsonDocument data) {
    return data.toString().getBytes(StandardCharsets.UTF_8);
  }
}
