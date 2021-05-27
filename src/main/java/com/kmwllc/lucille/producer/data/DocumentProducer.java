package com.kmwllc.lucille.producer.data;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.DocumentException;
import com.kmwllc.lucille.producer.FileTraverser;
import com.kmwllc.lucille.producer.data.producer.CSVDocumentProducer;
import com.kmwllc.lucille.producer.data.producer.DefaultDocumentProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

public interface DocumentProducer {
  Logger log = LogManager.getLogger(DocumentProducer.class);
  Base64.Encoder ENCODER = Base64.getEncoder();

  /**
   * If multiple documents are being produced, the {@param doc} should be cloned.
   *
   * @param file
   * @param doc
   * @return
   * @throws DocumentException
   */
  List<Document> produceDocuments(Path file, Document doc) throws DocumentException, IOException;

  // TODO: figure out how we'll do this later
  default boolean shouldDoFileSizeCheck() {
    return true;
  }

  // TODO: allow user to specify ID prefix (empty string by default)
  default String createId(String uniqueKey) {
    // TODO: MD5 this
    return ENCODER.encodeToString(uniqueKey.getBytes(StandardCharsets.UTF_8));
  }

  default Document createTombstone(Path file, Throwable e) throws DocumentException {
    Document doc = new Document(createId(file.toString()));
    doc.setField(FileTraverser.FILE_PATH, file.toString());
    doc.logError("Error occurred while loading document with id " + doc.getId() + ": " + e.getMessage());
    return doc;
  }

  static DocumentProducer getProducer(String type) {
    switch (type) {
      case "CSV":
        return new CSVDocumentProducer();
      case "DEFAULT":
        return new DefaultDocumentProducer();
      default:
        log.warn("Unknown type provided {}, using DEFAULT type. Known types are CSV, DEFAULT", type);
        return new DefaultDocumentProducer();
    }
  }
}
