package com.kmwllc.lucille.producer.data.producer;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.DocumentException;
import com.kmwllc.lucille.producer.FileTraverser;
import com.kmwllc.lucille.producer.data.DocumentProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

public class DefaultDocumentProducer implements DocumentProducer {
  private static final Logger log = LogManager.getLogger(DefaultDocumentProducer.class);
  public static final String CONTENT = "file_content";
  private static final Base64.Decoder DECODER = Base64.getDecoder();

  @Override
  public List<Document> produceDocuments(Path file, Document doc) {
    try {
      doc.setField(CONTENT, ENCODER.encodeToString(Files.readAllBytes(file)));
    } catch (IOException e) {
      String msg = String.format("Error occurred while loading document with id %s: %s", doc.getId(), e.getMessage());
      log.error(msg, e);
      doc.logError(msg);
    }

    return Collections.singletonList(doc);
  }

  public static byte[] decodeFileContents(Document doc) {
    if (!doc.has(CONTENT)) {
      // TODO: what do we actually want to do here?
      throw new NullPointerException("Document does not contain CONTENT field \"" + CONTENT + "\"");
    }
    return DECODER.decode(doc.getString(CONTENT));
  }
}
