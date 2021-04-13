package com.kmwllc.lucille.producer.datatype;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class FileInfoSerializer implements Serializer<FileInfo> {
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final Logger log = LogManager.getLogger(FileInfoSerializer.class);

  @Override
  public byte[] serialize(String topic, FileInfo data) {
    try {
      return mapper.writeValueAsBytes(data);
    } catch (JsonProcessingException e) {
      log.error("Error serializing JSON object", e);
      return null;
    }
  }
}
