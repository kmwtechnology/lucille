package com.kmwllc.lucille.core.fileHandlers;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.nio.file.Path;
import java.util.Iterator;

public abstract class BaseFileTypeHandler implements FileTypeHandler {
  protected String docIdPrefix;

  public BaseFileTypeHandler(Config config) {
    this.docIdPrefix = config.hasPath("docIdPrefix") ? config.getString("docIdPrefix") : "";
  }

  /**
   * processFileAndPublish is a default helper function that processes a file and publishes the documents to a Publisher
   * using Path to file.
   */
  public void processFileAndPublish(Publisher publisher, Path path) throws Exception {
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
  public void processFileAndPublish(Publisher publisher, byte[] fileContent, String pathStr) throws Exception {
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

  public void beforeProcessingFile(Path path) throws Exception {
    return;
  }

  public void beforeProcessingFile(byte[] content) throws Exception {
    return;
  }

  public void afterProcessingFile(Path path) throws Exception {
    return;
  }

  public void afterProcessingFile(byte[] content) throws Exception {
    return;
  }

  public void errorProcessingFile(Path path) {
    return;
  }

  public void errorProcessingFile(byte[] content) {
    return;
  }
}
