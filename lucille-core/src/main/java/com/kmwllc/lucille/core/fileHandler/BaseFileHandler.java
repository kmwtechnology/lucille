package com.kmwllc.lucille.core.fileHandler;
import com.kmwllc.lucille.connector.CSVConnector;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.nio.file.Path;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseFileHandler implements FileHandler {
  protected String docIdPrefix;
  private static final Logger log = LoggerFactory.getLogger(BaseFileHandler.class);

  public BaseFileHandler(Config config) {
    this.docIdPrefix = config.hasPath("docIdPrefix") ? config.getString("docIdPrefix") : "";
  }

  public void processFileAndPublish(Publisher publisher, Path path) throws FileHandlerException {
    Iterator<Document> docIterator;
    try {
      docIterator = processFile(path);
    } catch (Exception e) {
      throw new FileHandlerException("Unable to set up iterator for this file " + path, e);
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
        log.error("Error occurred while publishing file {}", path, e);
      }
    }
  }

  public void processFileAndPublish(Publisher publisher, byte[] fileContent, String pathStr) throws FileHandlerException {
    Iterator<Document> docIterator;
    try {
      docIterator = processFile(fileContent, pathStr);
    } catch (Exception e) {
      // going to skip this file if an error occurs
      throw new FileHandlerException("Unable to set up iterator for this file " + pathStr, e);
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
        log.error("Error occurred while publishing file {}", pathStr, e);
      }
    }
  }
}
