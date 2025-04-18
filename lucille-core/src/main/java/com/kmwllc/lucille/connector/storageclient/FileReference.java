package com.kmwllc.lucille.connector.storageclient;

import com.kmwllc.lucille.core.Document;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;

/**
 * Stores a reference to a file in a cloud storage service or a local file system.
 */
public interface FileReference {
  /**
   * @return The name of this file.
   */
  String getName();

  /**
   * @return A String representing the full path to this file. If the file is a cloud file, it will be written as a full URI
   * to the file. Local files will be an absolute, normalized path to the file.
   */
  String getFullPath();

  /**
   * @return The extension associated with this file.
   */
  String getFileExtension();

  /**
   * @return Whether this FileReference is valid, namely, whether it is a reference to an actual file and not
   * a Directory.
   */
  boolean isValidFile();

  /**
   * @return The instant at which this FileReference was last modified.
   */
  Instant getLastModified();

  /**
   * Returns an InputStream for the file's contents, using the given TraversalParams as needed.
   * <p> <b>Note:</b> The returned InputStream will not necessarily be buffered!
   *
   * @return An InputStream for the file's contents, using the given TraversalParams as needed.
   */
  InputStream getContentStream(TraversalParams params);

  /**
   * @return A Lucille Document from this file reference. Will get the file's contents if params.shouldGetFileContent()
   * is true.
   */
  Document asDoc(TraversalParams params);

  /**
   * @return A Lucille Document from this file reference, using the given full path string to create the Document's ID / path,
   * and reading all bytes from the given input stream if params.shouldGetFileContent() is true.
   */
  Document asDoc(InputStream in, String decompressedFullPathStr, TraversalParams params) throws IOException;
}
