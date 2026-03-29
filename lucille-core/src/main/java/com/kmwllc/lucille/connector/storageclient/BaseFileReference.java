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
  public String getFileExtension() {
    String fileName = getName();
    return FilenameUtils.getExtension(fileName);
  }

  /**
   * Returns a byte[] representing the file's contents.
   * @return a byte[] for the file's contents.
   */
  protected abstract byte[] getFileContent(TraversalParams params);

  /**
   * Builds a Document populated with the standard file metadata fields (file path, modification date, size, creation date).
   * The document ID is derived from an MD5 hash of the full path string combined with the traversal params.
   *
   * @param fullPath     the full path string of the file
   * @param lastModified the last modification time of the file
   * @param size         the size of the file in bytes
   * @param created      the creation time of the file
   * @param params       the traversal parameters used to derive the document ID
   * @return a Document with standard file metadata fields populated
   */
  public static Document buildBaseDoc(String fullPath, Instant lastModified, Long size, Instant created, TraversalParams params) {
    Document doc = createEmptyDocument(params, fullPath);

    doc.setField(FILE_PATH, fullPath);

    if (lastModified != null) {
      doc.setField(MODIFIED, lastModified);
    }

    if (size != null) {
      doc.setField(SIZE, size);
    }

    if (created != null) {
      doc.setField(CREATED, created);
    }

    return doc;
  }

  @Override
  public Document asDoc(TraversalParams params) {
    Document doc = buildBaseDoc(getFullPath().toString(), lastModified, size, created, params);

    if (params.shouldGetFileContent()) {
      doc.setField(CONTENT, getFileContent(params));
    }

    return doc;
  }

  @Override
  public Document decompressedFileAsDoc(InputStream in, String decompressedFullPathStr, TraversalParams params) throws IOException {
    Document doc = buildBaseDoc(decompressedFullPathStr, lastModified, size, created, params);

    if (params.shouldGetFileContent()) {
      doc.setField(CONTENT, in.readAllBytes());
    }

    return doc;
  }

  /**
   * Creates an empty document for this FileReference. Uses the given full path and params to create an appropriate docId.
   * @param params Parameters for your storage traversal.
   * @param fullPathString The full path String to the file reference
   * @return An empty Document with an appropriate docId representing this file reference.
   */
  protected static Document createEmptyDocument(TraversalParams params, String fullPathString) {
    String docId = DigestUtils.md5Hex(fullPathString);
    return Document.create(StorageClient.createDocId(docId, params));
  }
}
