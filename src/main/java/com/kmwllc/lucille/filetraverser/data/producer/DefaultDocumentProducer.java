package com.kmwllc.lucille.filetraverser.data.producer;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.filetraverser.data.DocumentProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

public class DefaultDocumentProducer implements DocumentProducer {
  private static final Logger log = LogManager.getLogger(DefaultDocumentProducer.class);
  public static final String CONTENT = "file_content";
  private static final Base64.Decoder DECODER = Base64.getDecoder();

  /**
   * @param childCopyParentMetadata Not in use yet...
   */
  public DefaultDocumentProducer(boolean childCopyParentMetadata) {}

  @Override
  public List<Document> produceDocuments(Path file, Document doc) {
    try {
      doc.setField(CONTENT, ENCODER.encodeToString(Files.readAllBytes(file)));
    } catch (IOException e) {
      String msg = String.format("Error occurred while loading document with id %s: %s", doc.getId(), e.getMessage());
      log.error(msg, e);
      doc.addToField("errors", msg);
    }

    return Collections.singletonList(doc);
  }

  /**
   * Decodes file contents sent by this {@link DocumentProducer} type from the provided {@link Document}.
   *
   * @param doc The document to extract binary data from
   * @return The byte[] binary data contained within the doc
   * @throws NullPointerException if the document doesn't contain the Content {@link this#CONTENT} field or the
   *                              field is null
   */
  public static byte[] decodeFileContents(Document doc) throws NullPointerException {
    if (!doc.hasNonNull(CONTENT)) {
      // TODO: what do we actually want to do here?
      throw new NullPointerException("Document does not contain CONTENT field \"" + CONTENT + "\"");
    }
    return DECODER.decode(doc.getString(CONTENT));
  }
}
