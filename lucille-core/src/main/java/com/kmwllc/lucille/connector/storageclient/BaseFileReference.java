package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.CONTENT;
import static com.kmwllc.lucille.connector.FileConnector.CREATED;
import static com.kmwllc.lucille.connector.FileConnector.FILE_PATH;
import static com.kmwllc.lucille.connector.FileConnector.MODIFIED;
import static com.kmwllc.lucille.connector.FileConnector.SIZE;

import com.kmwllc.lucille.core.Document;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;

public abstract class BaseFileReference implements FileReference {

  private final URI pathToFile;
  private final Instant lastModified;
  private final Long size;
  private final Instant created;

  public BaseFileReference(URI pathToFile, Instant lastModified, Long size, Instant created) {
    this.pathToFile = pathToFile;
    this.lastModified = lastModified;
    this.size = size;
    this.created = created;
  }

  @Override
  public URI getFullPath() { return pathToFile; }

  @Override
  public Instant getLastModified() {
    return this.lastModified;
  }

  @Override
  public String getFullPath() { return fullPathStr; }

  @Override
  public String getFileExtension() {
    String fileName = getName();
    return FilenameUtils.getExtension(fileName);
  }

  /**
   * Returns a byte[] representing the file's contents.
   * @return a byte[] for the file's contents.
   */
  protected abstract byte[] getFileContent(TraversalParams params);

  @Override
  public Document asDoc(TraversalParams params) {
    Document doc = createEmptyDocument(params);

    doc.setField(FILE_PATH, getFullPath().toString());

    if (lastModified != null) {
      doc.setField(MODIFIED, lastModified);
    }

    if (size != null) {
      doc.setField(SIZE, size);
    }

    if (created != null) {
      doc.setField(CREATED, created);
    }

    if (params.shouldGetFileContent()) {
      doc.setField(CONTENT, getFileContent(params));
    }

    return doc;
  }

  @Override
  public Document decompressedFileAsDoc(InputStream in, String decompressedFullPathStr, TraversalParams params) throws IOException {
    Document doc = createEmptyDocument(params, decompressedFullPathStr);

    doc.setField(FILE_PATH, decompressedFullPathStr);

    if (lastModified != null) {
      doc.setField(MODIFIED, lastModified);
    }

    if (size != null) {
      doc.setField(SIZE, size);
    }

    if (created != null) {
      doc.setField(CREATED, created);
    }

    if (params.shouldGetFileContent()) {
      doc.setField(CONTENT, in.readAllBytes());
    }

    return doc;
  }

  /**
   * Creates an empty document for this FileReference. Uses the full path of this FileReference
   * and the given params to create an appropriate docId.
   * @param params Parameters for your storage traversal.
   * @return An empty Document with an appropriate docId representing this file reference.
   */
  protected Document createEmptyDocument(TraversalParams params) {
    String fullPath = getFullPath().toString();
    return createEmptyDocument(params, fullPath);
  }

  /**
   * Creates an empty document for this FileReference. Uses the given full path and params to create an appropriate docId.
   * @param params Parameters for your storage traversal.
   * @param fullPathString The full path String to the file reference
   * @return An empty Document with an appropriate docId representing this file reference.
   */
  protected Document createEmptyDocument(TraversalParams params, String fullPathString) {
    String docId = DigestUtils.md5Hex(fullPathString);
    return Document.create(StorageClient.createDocId(docId, params));
  }
}
