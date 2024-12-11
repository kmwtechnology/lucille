package com.kmwllc.lucille.core.fileHandlers;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.nio.file.Path;
import java.util.Iterator;

public interface FileHandler {

  Iterator<Document> processFile(Path path) throws Exception;

  Iterator<Document> processFile(byte[] fileContent, String pathStr) throws Exception;

  default void processFileAndPublish(Publisher publisher, Path path) throws Exception {
    Iterator<Document> docs = processFile(path);
    while (docs.hasNext()) {
      Document doc = docs.next();
      if (doc != null) {
        publisher.publish(docs.next());
      }
    }
  }

  default void beforeProcessingFile(Config config, Path path) throws Exception {
    return;
  }

  default void afterProcessingFile(Config config, Path path) throws Exception {
    return;
  }

  default void errorProcessingFile(Config config, Path path) {
    return;
  }

  default void beforeProcessingFile(Config config, byte[] content) throws Exception {
    return;
  }

  default void afterProcessingFile(Config config, byte[] content) throws Exception {
    return;
  }

  default void errorProcessingFile(Config config, byte[] content) {
    return;
  }

  default void closeHandlerResources() {
    return;
  }

  static FileHandler getFileHandler(String fileExtension, Config fileOptions) {
    switch (fileExtension) {
      case "json", "jsonl" -> {
        Config jsonConfig = fileOptions.getConfig("json");
        if (jsonConfig == null || jsonConfig.isEmpty()) jsonConfig = fileOptions.getConfig("jsonl");

        return FileHandlerManager.getJsonHandler(jsonConfig);
      }
      case "csv" -> {
        Config csvConfig = fileOptions.getConfig("csv");
        return FileHandlerManager.getCsvHandler(csvConfig);
      }
      case "xml" -> {
        Config xmlConfig = fileOptions.getConfig("xml");
        return FileHandlerManager.getXmlHandler(xmlConfig);
      }
      default -> throw new RuntimeException("Unsupported file type: " + fileExtension);
    }
  }

  static boolean supportsFileType(String fileExtension, Config fileOptions) {
    switch (fileExtension) {
      case "json", "jsonl" -> {
        return fileOptions.hasPath("json") || fileOptions.hasPath("jsonl");
      }
      case "csv" -> {
        return fileOptions.hasPath("csv");
      }
      case "xml" -> {
        return fileOptions.hasPath("xml");
      }
      default -> {
        return false;
      }
    }
  }
}
