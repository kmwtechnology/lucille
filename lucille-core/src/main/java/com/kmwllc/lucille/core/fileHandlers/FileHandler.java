package com.kmwllc.lucille.core.fileHandlers;

import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.nio.file.Path;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface FileHandler {

  Logger log = LoggerFactory.getLogger(FileHandler.class);

  Iterator<Document> processFile(Path path) throws Exception;

  Iterator<Document> processFile(byte[] fileContent, String pathStr) throws Exception;

  default void beforeProcessingFile(Path path) throws Exception {
    return;
  }

  default void afterProcessingFile(Path path) throws Exception {
    return;
  }

  default void errorProcessingFile(Path path) {
    return;
  }

  default void beforeProcessingFile(byte[] content) throws Exception {
    return;
  }

  default void afterProcessingFile(byte[] content) throws Exception {
    return;
  }

  default void errorProcessingFile(byte[] content) {
    return;
  }

  default void closeHandlerResources() {
    return;
  }

  default void processFileAndPublish(Publisher publisher, Path path) throws Exception {
    Iterator<Document> docIterator;
    try {
      docIterator = this.processFile(path);
    } catch (Exception e) {
      // going to skip this file if an error occurs
      this.errorProcessingFile(path);
      throw new RuntimeException("Unable to set up iterator for this file " + path, e);
    }
    // once docIterator.hasNext() is false, it will close its resources in handler and return
    while (docIterator.hasNext()) {
      try {
        Document doc = docIterator.next();
        if (doc != null) {
          publisher.publish(doc);
        }
      } catch (Exception e) {
        // if we fail to publish a document, we log the error and continue to the next document
        // to "finish" the iterator and close its resources
        this.errorProcessingFile(path);
        throw new RuntimeException("Error occurred while publishing file " + path, e);
      }
    }
  }

  default void processFileAndPublish(Publisher publisher, byte[] fileContent, String pathStr) throws Exception {
    Iterator<Document> docIterator;
    try {
      docIterator = this.processFile(fileContent, pathStr);
    } catch (Exception e) {
      // going to skip this file if an error occurs
      this.errorProcessingFile(fileContent);
      throw new RuntimeException("Unable to set up iterator for this file " + pathStr, e);
    }
    // once docIterator.hasNext() is false, it will close its resources in handler and return
    while (docIterator.hasNext()) {
      try {
        Document doc = docIterator.next();
        if (doc != null) {
          publisher.publish(doc);
        }
      } catch (Exception e) {
        // if we fail to publish a document, we log the error and continue to the next document
        // to "finish" the iterator and close its resources
        this.errorProcessingFile(fileContent);
        throw new RuntimeException("Error occurred while publishing file " + pathStr, e);
      }
    }
  }

  static FileHandler getFileHandler(String fileExtension, Config fileOptions) {
    switch (fileExtension) {
      case "json", "jsonl" -> {
        Config jsonConfig = fileOptions.getConfig("json");
        if (jsonConfig == null) jsonConfig = fileOptions.getConfig("jsonl");

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
