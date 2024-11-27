package com.kmwllc.lucille.core;

import com.kmwllc.lucille.connector.JSONConnector;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.io.FilenameUtils;

public interface FileHandler {

  Iterator<Document> processFile(Path path) throws Exception;

  Iterator<Document> processFile(byte[] fileContent) throws Exception;

  static FileHandler getFileHandler(String fileExtension, Config fileOptions) {
    switch (fileExtension) {
      case "json", "jsonl" -> {
        Config jsonConfig = fileOptions.getConfig("json");
        if (jsonConfig == null || jsonConfig.isEmpty()) jsonConfig = fileOptions.getConfig("jsonl");

        return FileHandlerSingleton.getJsonConnectorInstance(jsonConfig);
      }
      default -> throw new RuntimeException("Unsupported file type: " + fileExtension);
    }
  }

  static boolean supportsFileType(String fileExtension, Config fileOptions) {
    switch (fileExtension) {
      case "json", "jsonl" -> {
        return fileOptions.hasPath("json") || fileOptions.hasPath("jsonl");
      }
      default -> {
        return false;
      }
    }
  }

  static void closeAllHandlers() {
    FileHandlerSingleton.closeAllHandlers();
  }
}
