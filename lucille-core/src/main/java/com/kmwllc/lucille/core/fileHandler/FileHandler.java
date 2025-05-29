package com.kmwllc.lucille.core.fileHandler;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
   * From the given Config, returns a map of supported file extensions to their FileHandlers.The returned map is not modifiable.
   * Configs mapped to <code>csv</code>, <code>xml</code>, <code>json</code>, and <code>jsonl</code> without a <code>class</code>
   * specified will use Lucille's <code>CSVFileHandler</code>, <code>XMLFileHandler</code>, or <code>JSONFileHandler</code>, respectively.
   * Configs mapped to another file extension (a custom <code>FileHandler</code>) must include <code>class</code> in their Config, or an Exception will be thrown.
   *
   * <br> <b>Note:</b> If <code>json</code> config is included, but <code>jsonl</code> is not (or vice versa), both extensions will
   * map to the same JSONFileHandler in the Config. If both are included, however, they will map to unique JSONFileHandlers,
   * created using their respective Configs.
   *
   * @param fileHandlersConfig The Config, typically under the key <code>fileHandlers</code>, which potentially contains Configs for various FileHandlers.
   * @return A map of file extensions to their respective file handlers, creating as many as could be built from the
   * provided config.
   */
  static Map<String, FileHandler> createFromConfig(Config fileHandlersConfig) {
    Map<String, FileHandler> handlerMap = new HashMap<>();

    for (String fileExtension : fileHandlersConfig.root().keySet()) {
      FileHandler handler = FileHandler.create(fileExtension, fileHandlersConfig);
      handlerMap.put(fileExtension, handler);
    }

    // only "adding" the other if it wasn't specified in the Config. Allows JSON / JSONL to be handled differently,
    // if need be...
    if (handlerMap.containsKey("json") && !handlerMap.containsKey("jsonl")) {
      handlerMap.put("jsonl", handlerMap.get("json"));
    } else if (handlerMap.containsKey("jsonl") && !handlerMap.containsKey("json")) {
      handlerMap.put("json", handlerMap.get("jsonl"));
    }

    return Collections.unmodifiableMap(handlerMap);
  }

  /**
   * Returns a new FileHandler based on the file extension and file options. If the file extension is not a "default" type supported
   * by Lucille, your Config must contain the class for the FileHandler.
   *
   * @param fileExtension The extension associated with the file.
   * @param fileHandlersConfig Configuration for fileHandlers. Should contain individual entries with configurations for the
   *                           different FileHandlers you want to support, keyed by their file extension. This config should contain
   *                           the given fileExtension.
   *
   * @return A FileHandler to process files with the given extension.
   * @throws IllegalArgumentException If you try to create a FileHandler for a non-default file type and don't specify a class, or
   * if a reflective error occurs instantiating a custom FileHandler class.
   */
  static FileHandler create(String fileExtension, Config fileHandlersConfig) {
    Config fileExtensionConfig = fileHandlersConfig.getConfig(fileExtension);

    // Allow for an overridden implementation, even if we provide a default implementation
    if (fileExtensionConfig.hasPath("class")) {
      String className = fileExtensionConfig.getString("class");

      try {
        Class<?> clazz = Class.forName(className);
        Constructor<?> constructor = clazz.getConstructor(Config.class);
        return (FileHandler) constructor.newInstance(fileExtensionConfig);
      } catch (ReflectiveOperationException e) {
        throw new IllegalArgumentException("Couldn't create custom FileHandler (" + fileExtension + ") from Config:", e);
      }
    }

    // No class provided, see if it is a supported type, throw an exception if it is not supported
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
        throw new IllegalArgumentException("No \"class\" provided for FileHandler config " + fileExtension + ".");
      }
    }
  }
}
