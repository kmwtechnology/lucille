package com.kmwllc.lucille.filetraverser.data;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.DocumentException;
import com.kmwllc.lucille.filetraverser.FileTraverser;
import com.kmwllc.lucille.filetraverser.data.producer.DefaultDocumentProducer;
import com.kmwllc.lucille.filetraverser.data.producer.OpenCSVDocumentProducer;
import com.kmwllc.lucille.filetraverser.data.kafkaserde.DocumentDeserializer;
import com.kmwllc.lucille.filetraverser.data.kafkaserde.DocumentSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.Locale;

/**
 * An interface for classes that handle the production of {@link Document}s by traversing a file
 * tree. These {@code Documents} can be sent to Kafka using a {@link
 * org.apache.kafka.clients.producer.Producer} set up with a {@link DocumentSerializer}, and
 * consumed using a {@link org.apache.kafka.clients.consumer.Consumer} set up with a {@link
 * DocumentDeserializer}.
 *
 * <p>Several methods are defined as {@code default} for convenience and may be overridden in the
 * implementing class, but the only required method is the {@link this#produceDocuments(Path,
 * Document)} method, which handles the creation of the documents from the given file.
 */
public interface DocumentProducer {
  Logger log = LogManager.getLogger(DocumentProducer.class);
  Base64.Encoder ENCODER = Base64.getEncoder();

  /**
   * Produces a list of documents from the given file. The {@code doc} param should be set up with
   * basic file information, so if multiple documents are going to be produced, the {@code doc}
   * should be cloned.
   *
   * <p>In the case where no documents should be produced by this file, an empty list should be
   * returned instead of {@code null}.
   *
   * <p>If an error occurs while processing the doc, the method should try to handle it on its own
   * and add a tombstone in the list of documents returned. If a {@link DocumentException} or {@link
   * IOException} occur, the FileTraverser attempts to send a basic tombstone for the document with
   * basic information. Therefore, if this error occurs while processing a child document, throwing
   * an error will prevent the processing of all remaining child documents, and data that may have
   * been usable will be skipped.
   *
   * @param file The file currently being processed
   * @param doc The document to-be-sent/cloned that should be set up with some basic information
   *     from the caller
   * @return A list of documents produced from the file, or an empty list if there are none
   * @throws DocumentException If a non-recoverable error occurs while processing the file
   */
  List<Document> produceDocuments(Path file, Document doc) throws DocumentException, IOException;

  /**
   * Returns true if the file's size should be checked before running document producer on file,
   * otherwise, no check will be performed and doc producer will always be run.
   *
   * <p>This check will not be performed if binary data is not being sent, since file size limits
   * and errors will only be applied when sending large file binary data.
   *
   * @return true if the file's size should be checked before running the document producer, false
   *     otherwise
   */
  default boolean shouldDoFileSizeCheck() {
    return true;
  }

  /**
   * Creates an ID by Base64 encoding an MD5 hash of the provided {@code uniqueKey}.
   *
   * @param uniqueKey The key uniquely identifying the document
   * @return A hashed, MD5 encoded value of the unique key
   */
  default String createId(String uniqueKey) {
    try {
      MessageDigest digest = MessageDigest.getInstance("MD5");
      return ENCODER.encodeToString(digest.digest(uniqueKey.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(
          "Could not instantiate MD5 message digest for ID creation", e);
    }
  }

  /**
   * Creates a tombstone of the provided document if an error occurs, logging the error in the
   * "errors" field and ensuring the {@link Document#ID_FIELD} and {@link FileTraverser#FILE_PATH}
   * fields are set.
   *
   * @param file The file to create a tombstone for
   * @param doc The document to create a tombstone on if one already exists, otherwise {@code null}
   * @param e The error that occurred while visiting/processing the document
   * @return A tombstone document
   * @throws DocumentException If an error occurred while creating a new tombstone document
   */
  default Document createTombstone(Path file, Document doc, Throwable e) throws DocumentException {
    if (doc == null) {
      doc = Document.create(createId(file.toString()));
    }
    doc.setField(FileTraverser.FILE_PATH, file.toString());
    doc.addToField(
        "errors",
        "Error occurred while loading document with id " + doc.getId() + ": " + e.getMessage());
    return doc;
  }

  /**
   * Creates a new Document producer based off of the provided {@code type} after uppercasing it. If
   * an unknown type is provided, a warning is logged and a {@link DefaultDocumentProducer} is
   * returned instead.
   *
   * @param type The type of producer to return, or the {@link DefaultDocumentProducer} if unknown
   * @return A new {@link DocumentProducer}
   */
  static DocumentProducer getProducer(String type, boolean childCopyParentMetadata) {
    switch (type.toUpperCase(Locale.ROOT)) {
      case "CSV":
        return new OpenCSVDocumentProducer(childCopyParentMetadata);
      case "DEFAULT":
        return new DefaultDocumentProducer(childCopyParentMetadata);
      default:
        log.warn(
            "Unknown type provided {}, using DEFAULT type. Known types are CSV, DEFAULT", type);
        return new DefaultDocumentProducer(childCopyParentMetadata);
    }
  }
}
