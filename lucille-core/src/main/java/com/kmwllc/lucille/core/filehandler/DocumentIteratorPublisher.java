package com.kmwllc.lucille.core.filehandler;

import static com.kmwllc.lucille.core.Document.ID_FIELD;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.MDC.MDCCloseable;

/**
 * Publishes the Documents of a {@link FileHandler#processFile} iterator, following the iterator contract described on
 * {@link FileHandler#processFile}. Backs the default {@link FileHandler#processFileAndPublish}.
 */
final class DocumentIteratorPublisher {

  private static final Logger log = LoggerFactory.getLogger(FileHandler.class);
  private static final Logger docLogger = LoggerFactory.getLogger("com.kmwllc.lucille.core.DocLogger");

  private DocumentIteratorPublisher() {
  }

  static void publishAll(Publisher publisher, Iterator<Document> docIterator, String pathStr) throws FileHandlerException {
    // once docIterator.hasNext() is false, it will close its resources in handler and return
    while (hasNext(docIterator, pathStr)) {
      Document doc;
      try {
        doc = docIterator.next();
      } catch (Exception e) {
        // a single bad record: the iterator stays usable, so skip it and carry on
        log.error("Error reading next document from file {}", pathStr, e);
        continue;
      }
      if (doc != null) {
        if (docLogger.isInfoEnabled()) {
          try (MDCCloseable docIdMDC = MDC.putCloseable(ID_FIELD, doc.getId())) {
            docLogger.info("FileHandler is now publishing Document {}", doc.getId());
          }
        }
        try {
          publisher.publish(doc);
        } catch (Exception e) {
          throw new FileHandlerException("Error publishing file " + pathStr, e);
        }
      }
    }
  }

  /**
   * Closes a stream given to a FileHandler, logging rather than throwing a failure to close it, since the file has been
   * read by then.
   */
  static void closeStream(InputStream inputStream, String pathStr) {
    try {
      inputStream.close();
    } catch (IOException e) {
      log.warn("Error closing file {}", pathStr, e);
    }
  }

  // a hasNext() that throws means the file can't be read any further
  private static boolean hasNext(Iterator<Document> docIterator, String pathStr) throws FileHandlerException {
    try {
      return docIterator.hasNext();
    } catch (BadRecordTracker.TooManyBadRecordsException e) {
      throw new FileHandlerException("Too many consecutive records could not be read from file " + pathStr, e);
    } catch (RuntimeException e) {
      throw new FileHandlerException("Error reading file " + pathStr, e);
    }
  }
}
