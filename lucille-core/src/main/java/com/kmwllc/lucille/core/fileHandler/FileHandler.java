package com.kmwllc.lucille.core.fileHandler;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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
  Set<String> SUPPORTED_FILE_TYPES = Set.of("json", "jsonl", "csv", "xml");

  /**
   * Processes a file given an InputStream of its contents and a representation path String to it, and returns an iterator
   * of Documents. The Iterator should close all resources when completed (hasNext() is false) or when an exception is thrown.
   * Path string is used for populating file path field of document and for logging/error/debugging purposes.
   *
   * @param inputStream An InputStream of the file's contents.
   * @param pathStr A string to the file you're processing, used for logging / debugging.
   * @return An Iterator of Documents extracted from the file's contents. Closes any resources when hasNext() is false.
   * @throws FileHandlerException If an error occurs setting up an iterator for the file.
   */
  Iterator<Document> processFile(InputStream inputStream, String pathStr) throws FileHandlerException;

  /**
   * A helper function that processes a file and publishes the documents to a Publisher using the content in the given
   * InputStream. The given stream should be closed as part of this function's operations (namely the Iterators created)
   *
   * @param publisher The publisher you want to publish documents to.
   * @param inputStream An InputStream of the file's contents.
   * @param pathStr A string to the file you're processing, used for logging / debugging.
   * @throws FileHandlerException If an error occurs setting up an iterator for the fiel.
   */
  void processFileAndPublish(Publisher publisher, InputStream inputStream, String pathStr) throws FileHandlerException;

  /**
   * Returns a new FileHandler based on the file extension and file options. Note that if you add support
   * for a new file type, you must also add the corresponding handler here AND in SUPPORTED_FILE_TYPES collection
   *
   * @param fileExtension The extension associated with the file.
   * @param fileOptions Configuration for how you want to handle / process files. Should contain individaul maps with
   *                    configuration for the different FileHandlers you want to support.
   *
   * @throws UnsupportedOperationException If you try to create a FileHandler for an unsupported file type.
   * @return A FileHandler to process files with the given extension.
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
      default -> throw new UnsupportedOperationException("Unsupported file type: " + fileExtension);
    }
  }

  /**
   * Returns a Map from the given Config, creating FileHandlers that can be constructed from the given config, mapped
   * to their corresponding file extensions. If json is included, jsonl will be as well (and vice versa).
   * The returned map is not modifiable.
   *
   * @param optionsWithHandlers A config that contains individual maps / configuration for the file handlers that you want
   *                            to support.
   * @return A map of file extensions to their respective file handlers, creating as many as could be built from the
   * provided config.
   */
  static Map<String, FileHandler> createFromConfig(Config optionsWithHandlers) {
    Map<String, FileHandler> handlerMap = new HashMap<>();

    for (String fileExtensionSupported : SUPPORTED_FILE_TYPES) {
      if (supportAndContainFileType(fileExtensionSupported, optionsWithHandlers)) {
        FileHandler handler = FileHandler.create(fileExtensionSupported, optionsWithHandlers);
        handlerMap.put(fileExtensionSupported, handler);
      }
    }

    return Collections.unmodifiableMap(handlerMap);
  }

  /**
   * supportAndContainFileType returns true if the file extension is in SUPPORTED_FILE_TYPES and
   * the file options contain the file extension. Handles the special case of json and jsonl files.
   *
   * @param fileExtension The extension of the file you want to check.
   * @param fileOptions A config that contains individual maps / configuration for the file handlers that you want
   *                    to support.
   * @return Whether the file type is supported by Lucille and your fileOptions contains configuration for the file type.
   */
  static boolean supportAndContainFileType(String fileExtension, Config fileOptions) {
    return SUPPORTED_FILE_TYPES.contains(fileExtension) &&
       (fileOptions.hasPath(fileExtension) ||
       (fileExtension.equals("json") && fileOptions.hasPath("jsonl")) ||
       (fileExtension.equals("jsonl") && fileOptions.hasPath("json")));
  }
}
