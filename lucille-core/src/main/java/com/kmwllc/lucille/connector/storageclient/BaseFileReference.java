package com.kmwllc.lucille.connector.storageclient;

import com.kmwllc.lucille.core.Document;
import java.time.Instant;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;

public abstract class BaseFileReference implements FileReference {

  private final String fullPathStr;
  private final Instant lastModified;

  public BaseFileReference(String fullPathStr, Instant lastModified) {
    this.fullPathStr = fullPathStr;
    this.lastModified = lastModified;
  }

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
   * Creates an empty document for this FileReference. Uses the full path of this FileReference
   * and the given params to create an appropriate docId.
   * @param params Parameters for your storage traversal.
   * @return An empty Document with an appropriate docId representing this file reference.
   */
  protected Document createEmptyDocument(TraversalParams params) {
    return createEmptyDocument(params, fullPathStr);
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
