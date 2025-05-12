package com.kmwllc.lucille.core.fileHandler;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Each implementation of the FileHandler handles a specific file type and is able to process the file and return an Iterator
 * of documents. It can also publish straight to the Lucille pipeline if given a Publisher.
 */
public interface FileHandler {

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
   * Returns a Map from the given Config, creating FileHandlers that can be constructed from the given config, mapped
   * to their corresponding file extensions. If json is included, jsonl will be as well (and vice versa) - both will map
   * to the same JSONFileHandler in the Config. The returned map is not modifiable.
   *
   * @param fileHandlersConfig The Config under key "fileHandlers" which potentially contains Configs for various FileHandlers.
   *
   * @return A map of file extensions to their respective file handlers, creating as many as could be built from the
   * provided config. If JSON is included in the fileHandlersConfig, JSONL will be supported in the returned Map as well.
   */
  static Map<String, FileHandler> createFromConfig(Config fileHandlersConfig) {
    Map<String, FileHandler> handlerMap = new HashMap<>();

    for (String fileExtension : fileHandlersConfig.root().keySet()) {
      FileHandler handler = FileHandler.create(fileExtension, fileHandlersConfig);
      handlerMap.put(fileExtension, handler);
    }

    if (handlerMap.containsKey("json")) {
      handlerMap.put("jsonl", handlerMap.get("json"));
    } else if (handlerMap.containsKey("jsonl")) {
      handlerMap.put("json", handlerMap.get("jsonl"));
    }

    return Collections.unmodifiableMap(handlerMap);
  }

  /**
   * Returns a new FileHandler based on the file extension and file options.
   *
   * @param fileExtension The extension associated with the file.
   * @param fileHandlersConfig Configuration for how you want to handle / process files. Should contain individual entries with
   *                    configurations for the different FileHandlers you want to support.
   *
   * @return A FileHandler to process files with the given extension.
   * @throws UnsupportedOperationException If you try to create a FileHandler for an unsupported file type.
   */
  // TODO: Should take in just the "fileHandlers" Config.
  static FileHandler create(String fileExtension, Config fileHandlersConfig) {
    Config fileExtensionConfig = fileHandlersConfig.getConfig(fileExtension);

    switch (fileExtension) {
      case "json", "jsonl" -> {
        return new JsonFileHandler(fileExtensionConfig);
      }
      case "csv" -> {
        return new CSVFileHandler(fileExtensionConfig);
      }
      case "xml" -> {
        return new XMLFileHandler(fileExtensionConfig);
      }
      default -> {
        try {
          String className = fileExtensionConfig.getString("class");
          Class<?> clazz = Class.forName(className);
          Constructor<?> constructor = clazz.getConstructor(Config.class);
          return (FileHandler) constructor.newInstance(fileExtensionConfig);
        } catch (ReflectiveOperationException e) {
          throw new IllegalArgumentException("Couldn't create custom FileHandler (" + fileExtension + ") from Config:", e);
        }
      }
    }
  }
}
