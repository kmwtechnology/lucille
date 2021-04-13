package com.kmwllc.lucille.producer.datatype;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class FileInfoDeserializer implements Deserializer<FileInfo> {
  private static final Logger log = LogManager.getLogger(FileInfoDeserializer.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  @Override
  public FileInfo deserialize(String topic, byte[] data) {
    try {
      return mapper.readValue(data, FileInfo.class);
    } catch (IOException e) {
      log.error("Error deserializing FileInfo value", e);
      return null;
    }
  }
}
