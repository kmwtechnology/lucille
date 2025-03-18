package com.kmwllc.lucille.connector.storageclient.filereference;

import com.azure.storage.blob.models.BlobItem;
import com.google.cloud.storage.Blob;
import com.kmwllc.lucille.connector.storageclient.TraversalParams;
import com.kmwllc.lucille.core.Document;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Instant;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Stores a reference to a file in a cloud storage service or a local file system.
 */
public interface FileReference {
  String getFilePath(TraversalParams params);

  boolean isCloudFileReference();

  boolean validFile();

  Instant getLastModified();

  /**
   * Creates a Lucille Document from the given file reference. Will not get the file's contents / put them on a doc,
   * even if params.shouldGetFileContent() is true.
   */
  Document toDoc(TraversalParams params);

  /**
   * Creates a Lucille Document from the given file reference. Will place the given byte[] into the "file_content" field
   * if params.shouldGetFileContent() is true.
   */
  Document toDoc(TraversalParams params, byte[] fileContents);

  Document toDoc(InputStream in, String decompressedFullPathStr, TraversalParams params) throws IOException;
}
