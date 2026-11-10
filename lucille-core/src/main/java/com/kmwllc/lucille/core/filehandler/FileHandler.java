package com.kmwllc.lucille.core.filehandler;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

/**
 * Each implementation of the FileHandler handles a specific file type and is able to process the file and return an Iterator
 * of documents. It can also publish straight to the Lucille pipeline if given a Publisher.
 *
 * <p><b>Threads:</b> a FileHandler need not be thread-safe, and Lucille's handlers are not (XMLFileHandler, for one, keeps
 * its parser in fields). Lucille creates one instance for each thread that uses it, so a handler is only ever called
 * from one thread at a time. A caller sharing handlers across threads must do the same.
 *
 * <p><b>Iterator or publish only:</b> a handler that can only publish, because its format isn't read through an Iterator,
 * returns false from {@link #supportsProcessFile()}, so that callers needing an Iterator can reject it when they start.
 */
public interface FileHandler {

  /**
   * Whether this handler supports {@link #processFile}. A handler that returns false supports only
   * {@link #processFileAndPublish}, which it must override, and throws from {@link #processFile}. Callers that need an
   * Iterator, such as the ApplyFileHandlers stage, check this when they start, so a misconfiguration is reported then,
   * instead of when the first file arrives.
   *
   * @return true, unless this handler can only publish.
   */
  default boolean supportsProcessFile() {
    return true;
  }

  /**
   * Processes a file given an InputStream of its contents and a representation path String to it, and returns an iterator
   * of Documents. Path string is used for populating file path field of document and for logging/error/debugging purposes.
   *
   * <p>The returned Iterator must follow this contract, which {@link #processFileAndPublish} relies on:
   * <ul>
   *   <li><b>A bad record:</b> when one record can't be turned into a Document, <code>next()</code> may throw. The
   *   Iterator must stay usable afterwards: the caller logs and skips that record, then calls <code>hasNext()</code> again
   *   to carry on with the next one.</li>
   *   <li><b>A file that can't be read:</b> when reading can't continue (an I/O error, a file in the wrong format, too
   *   many bad records in a row), <code>hasNext()</code> throws a RuntimeException, and the caller stops reading the file.</li>
   *   <li><code>next()</code> may return null, which the caller skips.</li>
   *   <li>The Iterator closes its resources, including the given InputStream, when <code>hasNext()</code> returns false
   *   and before <code>hasNext()</code> throws. It keeps them open when <code>next()</code> throws for a bad record,
   *   because reading carries on.</li>
   * </ul>
   *
   * @param inputStream An InputStream of the file's contents.
   * @param pathStr A string to the file you're processing, used for logging / debugging.
   * @return An Iterator of Documents extracted from the file's contents, following the contract above.
   * @throws FileHandlerException If an error occurs setting up an iterator for the file.
   */
  Iterator<Document> processFile(InputStream inputStream, String pathStr) throws FileHandlerException;

  /**
   * Processes a file and publishes the documents to a Publisher using the content in the given InputStream.
   *
   * <p>The default implementation iterates {@link #processFile} and publishes each Document, following the iterator
   * contract described there: a record whose <code>next()</code> throws is logged and skipped, and a <code>hasNext()</code>
   * that throws stops the file. Override it only for a format that is not read through an Iterator, or that has to manage
   * its resources differently.
   *
   * <p>The given InputStream is closed when this returns or throws, including when the Publisher throws partway through
   * the file. An override must do the same.
   *
   * @param publisher The publisher you want to publish documents to.
   * @param inputStream An InputStream of the file's contents.
   * @param pathStr A string to the file you're processing, used for logging / debugging.
   * @throws FileHandlerException If an error occurs setting up an iterator for the file, if the file can't be read, or if
   * publishing a Document fails. The exception thrown by the Iterator or the Publisher is the cause.
   */
  default void processFileAndPublish(Publisher publisher, InputStream inputStream, String pathStr) throws FileHandlerException {
    // the Iterator closes the stream when it finishes, but not when publishing stops partway, such as when the Publisher
    // throws: an open stream from a cloud provider holds an HTTP connection
    try (inputStream) {
      Iterator<Document> docIterator;

      try {
        docIterator = processFile(inputStream, pathStr);
      } catch (Exception e) {
        // going to skip this file if an error occurs
        throw new FileHandlerException("Unable to set up iterator for this file " + pathStr, e);
      }

      DocumentIteratorPublisher.publishAll(publisher, docIterator, pathStr);
    } catch (IOException e) {
      throw new FileHandlerException("Error closing file " + pathStr, e);
    }
  }

  /**
   * From the given Config, returns a map of supported file extensions to their FileHandlers.The returned map is not modifiable.
   * Configs mapped to <code>csv</code>, <code>xml</code>, <code>json</code>, and <code>jsonl</code> without a <code>class</code>
   * specified will use Lucille's <code>CSVFileHandler</code>, <code>XMLFileHandler</code>, or <code>JsonFileHandler</code>, respectively.
   * Configs mapped to another file extension (a custom <code>FileHandler</code>) must include <code>class</code> in their Config, or an Exception will be thrown.
   *
   * <br> <b>Note:</b> If <code>json</code> config is included, but <code>jsonl</code> is not (or vice versa), both extensions will
   * map to the same JsonFileHandler in the Config. If both are included, however, they will map to unique JsonFileHandlers,
   * created using their respective Configs.
   *
   * @param fileHandlersConfig The Config, typically under the key <code>fileHandlers</code>, which potentially contains Configs for various FileHandlers.
   * @return A map of file extensions to their respective file handlers, creating as many as could be built from the
   * provided config.
   */
  static Map<String, FileHandler> createFromConfig(Config fileHandlersConfig) {
    Map<String, FileHandler> handlerMap = new HashMap<>();

    // keys are extensions, matched without regard to case, so "CSV" and "csv" name the same handler
    Map<String, String> configuredKeys = new HashMap<>();
    for (String configKey : fileHandlersConfig.root().keySet()) {
      String fileExtension = configKey.toLowerCase(Locale.ROOT);
      String previousKey = configuredKeys.put(fileExtension, configKey);
      if (previousKey != null) {
        throw new IllegalArgumentException("fileHandlers has both \"" + previousKey + "\" and \"" + configKey
            + "\". Extensions are matched without regard to case, so only one may be configured.");
      }

      handlerMap.put(fileExtension, FileHandler.create(configKey, fileHandlersConfig));
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
    switch (fileExtension.toLowerCase(Locale.ROOT)) {
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
