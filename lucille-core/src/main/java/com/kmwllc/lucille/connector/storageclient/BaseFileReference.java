package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.CONTENT;
import static com.kmwllc.lucille.connector.FileConnector.CREATED;
import static com.kmwllc.lucille.connector.FileConnector.FILE_PATH;
import static com.kmwllc.lucille.connector.FileConnector.MODIFIED;
import static com.kmwllc.lucille.connector.FileConnector.SIZE;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.util.FileUtils;
import com.kmwllc.lucille.util.FileUtils.ContentTooLargeException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
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
    String docId = DigestUtils.md5Hex(fullPath);
    Document doc = Document.create(StorageClient.createDocId(docId, params));

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
      doc.setField(CONTENT, getBoundedFileContent(params));
    }

    return doc;
  }

  // checks the listed size first so an oversized cloud file is never downloaded; the bounded read covers a missing or
  // stale size
  private byte[] getBoundedFileContent(TraversalParams params) {
    Long maxSizeBytes = params.getMaxSizeBytes();

    if (maxSizeBytes == null) {
      return getFileContent(params);
    }

    if (size != null && size > maxSizeBytes) {
      throw new UncheckedIOException(new ContentTooLargeException(
          String.format("File of %d bytes exceeds maxSizeBytes of %d. (%s)", size, maxSizeBytes, pathToFile)));
    }

    try (InputStream in = getContentStream(params)) {
      return FileUtils.readAllBytes(in, maxSizeBytes);
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Error reading file content. (%s)", pathToFile), e);
    }
  }

  @Override
  public Document decompressedFileAsDoc(InputStream in, String decompressedFullPathStr, TraversalParams params) throws IOException {
    Document doc = buildBaseDoc(decompressedFullPathStr, lastModified, size, created, params);

    if (params.shouldGetFileContent()) {
      // the size of this reference is the compressed size, so it can't be used to check the limit up front
      doc.setField(CONTENT, FileUtils.readAllBytes(in, params.getMaxSizeBytes()));
    }

    return doc;
  }

}
