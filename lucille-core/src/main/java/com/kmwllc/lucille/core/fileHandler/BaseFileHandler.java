package com.kmwllc.lucille.core.fileHandler;

import static com.kmwllc.lucille.core.Document.ID_FIELD;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.Spec;
import com.typesafe.config.Config;
import java.io.InputStream;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.MDC.MDCCloseable;

/**
 * The base implementation of a FileHandler.
 */
public abstract class BaseFileHandler implements FileHandler {

  private static final Logger log = LoggerFactory.getLogger(BaseFileHandler.class);
  private static final Logger docLogger = LoggerFactory.getLogger("com.kmwllc.lucille.core.DocLogger");

  protected final String docIdPrefix;

  public BaseFileHandler(Config config, Spec spec) {
    spec.validate(config, this.getClass().getName());

    this.docIdPrefix = config.hasPath("docIdPrefix") ? config.getString("docIdPrefix") : "";
  }

  /**
   * Process and publish the file, described by an InputStream to its contents and a String representation of its path, publishing
   * any extracted documents to the Publisher.
   *
   * @param publisher The publisher you want to publish documents to.
   * @param inputStream An InputStream of the file's contents.
   * @param pathStr A string representing the path to the file being processed. Used for logging / debugging.
   * @throws FileHandlerException If an error occurs while processing the file / setting up an Iterator of Documents
   * to extract from the file.
   */
  public void processFileAndPublish(Publisher publisher, InputStream inputStream, String pathStr) throws FileHandlerException {
    Iterator<Document> docIterator;

    try {
      docIterator = processFile(inputStream, pathStr);
    } catch (Exception e) {
      // going to skip this file if an error occurs
      throw new FileHandlerException("Unable to set up iterator for this file " + pathStr, e);
    }

    // once docIterator.hasNext() is false, it will close its resources in handler and return
    while (docIterator.hasNext()) {
      try {
        Document doc = docIterator.next();
        if (doc != null) {
          try (MDCCloseable docIdMDC = MDC.putCloseable(ID_FIELD, doc.getId())) {
            docLogger.info("FileHandler is now publishing Document {}", doc.getId());
          }
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
