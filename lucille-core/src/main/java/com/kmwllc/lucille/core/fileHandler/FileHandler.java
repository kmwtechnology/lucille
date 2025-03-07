package com.kmwllc.lucille.core.fileHandler;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.Iterator;
import java.util.Set;

/**
 * Each implementation of the FileHandler handles a specific file type and is able to process the file and return an Iterator
 * of documents. It can also publish straight to the Lucille pipeline if given a Publisher.
 */

public interface FileHandler {

  /**
   * Collection of file types that are supported by the FileHandler interface.
   * Note that if you add a new file type to the collection, you must also add the corresponding handler in create() method
   */
  Set<String> SUPPORTED_FILE_TYPES = Set.of("json", "jsonl", "csv", "xml", "parquet");

  /**
   * Processes a file given an InputStream of its contents and a representation path String to it, and returns an iterator
   * of Documents. The Iterator should close all resources when completed (hasNext() is false) or when an exception is thrown.
   * Path string is used for populating file path field of document and for logging/error/debugging purposes.
   */
  Iterator<Document> processFile(InputStream inputStream, String pathStr) throws FileHandlerException;

  /**
   * A helper function that processes a file and publishes the documents to a Publisher using the content in the given
   * InputStream. The given stream should be closed as part of this function's operations (namely the Iterators created)
   */
  void processFileAndPublish(Publisher publisher, InputStream inputStream, String pathStr) throws FileHandlerException;

  /**
   * Returns a new FileHandler based on the file extension and file options. Note that if you add support
   * for a new file type, you must also add the corresponding handler here AND in SUPPORTED_FILE_TYPES collection
   */
  static FileHandler create(String fileExtension, Config fileOptions) {
    switch (fileExtension) {
      case "json", "jsonl" -> {
        Config jsonConfig = fileOptions.hasPath("json") ? fileOptions.getConfig("json") : fileOptions.getConfig("jsonl");

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
      case "parquet" -> {
        Config parquetConfig = fileOptions.getConfig("parquet");

        try {
          Class<?> parquetFileHandlerClass = Class.forName("com.kmwllc.lucille.parquet.core.fileHandler.ParquetFileHandler");
          Constructor<?> constructor = parquetFileHandlerClass.getConstructor(Config.class);
          return (FileHandler) constructor.newInstance(parquetConfig);
        } catch (ReflectiveOperationException e) {
          throw new UnsupportedOperationException("Reflection error while constructing ParquetFileHandler, is the Parquet plugin included correctly?", e);
        }
      }
      default -> throw new UnsupportedOperationException("Unsupported file type: " + fileExtension);
    }
  }

  /**
   * supportAndContainFileType returns true if the file extension is in SUPPORTED_FILE_TYPES and
   * the file options contain the file extension. Handles the special case of json and jsonl files.
   */
  static boolean supportAndContainFileType(String fileExtension, Config fileOptions) {
    return SUPPORTED_FILE_TYPES.contains(fileExtension) &&
       (fileOptions.hasPath(fileExtension) ||
       (fileExtension.equals("json") && fileOptions.hasPath("jsonl")) ||
       (fileExtension.equals("jsonl") && fileOptions.hasPath("json")));
  }
}
