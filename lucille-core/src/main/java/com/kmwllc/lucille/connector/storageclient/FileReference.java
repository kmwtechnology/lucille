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
   * @return A String representing the full path to this file.
   */
  String getFullPath(TraversalParams params);

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
   * @return The instant at which this FileReference was last modified. May be null.
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
   * @return A Lucille Document from this file reference. Will retrieve the file's contents if params.shouldGetFileContent()
   * is true.
   */
  Document asDoc(TraversalParams params);

  /**
   * @return A Lucille Document for an archive or compressed file that came from this file reference.
   * The Document's ID will be a hash of the given path, and the Document's "file_path" will be the given path. However,
   * the Document's lastModified, size, and creation time will be that of this FileReference.
   * The input stream will be used to get the file's content, if {@link TraversalParams#shouldGetFileContent()} is true.
   */
  Document decompressedFileAsDoc(InputStream in, String decompressedFullPathStr, TraversalParams params) throws IOException;
}
