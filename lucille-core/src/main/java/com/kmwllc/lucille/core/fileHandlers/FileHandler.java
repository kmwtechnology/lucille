package com.kmwllc.lucille.core.fileHandlers;

import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface FileHandler {

  /**
   * SUPPORTED_FILE_TYPES is a list of file types that are supported by the FileHandler interface.
   * Note that if you add to SUPPORTED_FILE_TYPES, you must also add the corresponding handler in getNewFileHandler
   */
  List<String> SUPPORTED_FILE_TYPES = List.of("json", "jsonl", "csv", "xml");

  /**
   * processes a file given the Path to it and returns an iterator of Documents.
   */
  Iterator<Document> processFile(Path path) throws Exception;

  /**
   * processes a file given the file contents and representation path string to it and returns an iterator of Documents.
   * Path string is used for populating file path field of document and for logging/error/debugging purposes.
   */
  Iterator<Document> processFile(byte[] fileContent, String pathStr) throws Exception;

  /**
   * performs any necessary setup before processing a file.
   */
  default void beforeProcessingFile(Path path) throws Exception {
    return;
  }

  /**
   * performs any necessary setup after processing a file.
   */
  default void afterProcessingFile(Path path) throws Exception {
    return;
  }

  /**
   * performs any necessary setup if handler encountered any error processing a file.
   */
  default void errorProcessingFile(Path path) {
    return;
  }

  /**
   * performs any necessary setup before processing a file.
   */
  default void beforeProcessingFile(byte[] content) throws Exception {
    return;
  }

  /**
   * performs any necessary setup after processing a file.
   */
  default void afterProcessingFile(byte[] content) throws Exception {
    return;
  }

  /**
   * performs any necessary setup if handler encountered any error processing a file.
   */
  default void errorProcessingFile(byte[] content) {
    return;
  }

  /**
   * processFileAndPublish is a default helper function that processes a file and publishes the documents to a Publisher
   * using Path to file.
   */
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

  /**
   * processFileAndPublish is a default helper function that processes a file and publishes the documents to a Publisher
   * using byte[] content.
   */
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

  /**
   * getNewFileHandler returns a new FileHandler based on the file extension and file options.
   * Note that if you add support for a new file type, you must also add the corresponding handler here
   * AND in SUPPORTED_FILE_TYPES
   */
  static FileHandler getNewFileHandler(String fileExtension, Config fileOptions) {
    switch (fileExtension) {
      case "json", "jsonl" -> {
        Config jsonConfig = fileOptions.getConfig("json");
        if (jsonConfig == null) jsonConfig = fileOptions.getConfig("jsonl");

        return new JsonFileHandler(jsonConfig);
      }
      case "csv" -> {
        Config csvConfig = fileOptions.getConfig("csv");
        return new CSVFileHandler(csvConfig);
      }
      case "xml" -> {
        Config xmlConfig = fileOptions.getConfig("xml");
        return new XMLFileHandler(xmlConfig);
      }
      default -> throw new RuntimeException("Unsupported file type: " + fileExtension);
    }
  }

  /**
   * supportAndContainFileType returns true if the file extension is in SUPPORTED_FILE_TYPES and
   * the file options contain the file extension.
   */
  static boolean supportAndContainFileType(String fileExtension, Config fileOptions) {
    return SUPPORTED_FILE_TYPES.contains(fileExtension) &&
       (fileOptions.hasPath(fileExtension) ||
       (fileExtension.equals("json") && fileOptions.hasPath("jsonl")) ||
       (fileExtension.equals("jsonl") && fileOptions.hasPath("json")));
  }
}
