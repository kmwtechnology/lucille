package com.kmwllc.lucille.core;

import com.kmwllc.lucille.connector.JSONConnector;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;

public interface FileHandler {

  Iterator<Document> processFile(Path path) throws Exception;

  Iterator<Document> processFile(byte[] fileContent) throws Exception;

  static FileHandler getFileHandler(String fileExtension, Map<String, Object> fileOptions) {
    switch (fileExtension) {
      case "json", "jsonl" -> {
        Map<String, Object> configMap = (Map<String, Object>) fileOptions.get("json");
        return FileHandlerSingleton.getJsonConnectorInstance(ConfigFactory.parseMap(configMap));
      }
      default -> throw new RuntimeException("Unsupported file type: " + fileExtension);
    }
  }

  static boolean supportsFileType(String fileExtension, Map<String, Object> fileOptions) {
    switch (fileExtension) {
      case "json", "jsonl" -> {
        return fileOptions.containsKey("json");
      }
      default -> {
        return false;
      }
    }
  }
}
