package com.kmwllc.lucille.core.fileHandler;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Set;

/**
 * each implementation of the FileHandler handles a specific file type
 */

public interface FileHandler {

  /**
   * collection of file types that are supported by the FileHandler interface.
   * Note that if you add a new file type to list, you must also add the corresponding handler in getNewFileHandler
   */
  Set<String> SUPPORTED_FILE_TYPES = Set.of("json", "jsonl", "csv", "xml");

  /**
   * processes a file given the Path to it and returns an iterator of Documents. The Iterator should close all resources
   * when completed (hasNext() is false) or when an exception is thrown.
   */
  Iterator<Document> processFile(Path path) throws FileHandlerException;

  /**
   * processes a file given the file contents and representation path string to it and returns an iterator of Documents.
   * The Iterator should close all resources when completed (hasNext() is false) or when an exception is thrown.
   * Path string is used for populating file path field of document and for logging/error/debugging purposes.
   */
  Iterator<Document> processFile(byte[] fileContent, String pathStr) throws FileHandlerException;

  /**
   * A helper function that processes a file and publishes the documents to a Publisher
   * using Path to file.
   */
  void processFileAndPublish(Publisher publisher, Path path) throws FileHandlerException;

  /**
   * A helper function that processes a file and publishes the documents to a Publisher
   * using byte[] content.
   */
  void processFileAndPublish(Publisher publisher, byte[] fileContent, String pathStr) throws FileHandlerException;

  /**
   * getNewFileHandler returns a new FileHandler based on the file extension and file options.
   * Note that if you add support for a new file type, you must also add the corresponding handler here
   * AND in SUPPORTED_FILE_TYPES
   */
  static FileHandler create(String fileExtension, Config fileOptions) {
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
