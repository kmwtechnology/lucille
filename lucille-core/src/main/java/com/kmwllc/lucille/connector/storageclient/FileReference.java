package com.kmwllc.lucille.connector.storageclient;

import com.kmwllc.lucille.core.Document;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Instant;

/**
 * Stores a reference to a file in a cloud storage service or a local file system.
 */
public interface FileReference {
  /**
   * @return The file's name: the last segment of its path, without any folder or prefix. For
   * <code>s3://bucket/folder/a.json</code>, this is <code>a.json</code>.
   */
  String getName();

  /**
   * Returns the name that <code>filterOptions.includes</code> and <code>filterOptions.excludes</code> are matched against.
   * Defaults to {@link #getName()}. The built-in clients for object stores (S3, Google Cloud, Azure) return the object key
   * or blob name, which includes any folder prefix (<code>folder/a.json</code>), so that patterns can match on folders.
   *
   * @return The name filter patterns are matched against.
   */
  default String getFilterName() {
    return getName();
  }

  /**
   * Returns the full path to this file. FileConnector derives the Document ID and the state key for the file from
   * <code>getFullPath().toString()</code>, so it must be a canonical, percent-encoded URI that is the same on every
   * run and platform for the same file. If it changes, the file is published under a new ID and is treated as new
   * by incremental state.
   *
   * @return A URI representing the full path to this file.
   */
  URI getFullPath();

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
   * Returns the instant at which this file was last modified, or null if the storage provider doesn't report it. A file
   * with no last-modified time passes <code>filterOptions.lastModifiedCutoff</code>, and in incremental mode is treated as
   * modified since it was last published, so it is published on every run.
   *
   * @return The instant at which this file was last modified. May be null.
   */
  Instant getLastModified();

  /**
   * Returns an InputStream for the file's contents, using the given TraversalParams as needed.
   * <p> <b>Note:</b> The returned InputStream will not necessarily be buffered!
   *
   * @param params Parameters for the traversal.
   * @return An InputStream for the file's contents, using the given TraversalParams as needed.
   * @throws IOException If an error occurs opening the file's contents.
   */
  InputStream getContentStream(TraversalParams params) throws IOException;

  /**
   * @param params Parameters for the traversal.
   * @return A Lucille Document from this file reference. Will retrieve the file's contents if params.shouldGetFileContent()
   * is true. Will include the file's path, and may include the file's last modified time, size, and creation time, if specified.
   * @throws IOException If an error occurs reading the file's contents, or they exceed <code>fileOptions.maxSizeBytes</code>.
   */
  Document asDoc(TraversalParams params) throws IOException;

  /**
   * @return A Lucille Document for an archive or compressed file that came from this file reference.
   * The Document's ID will be a hash of the given path, and the Document's "file_path" will be the given path. However,
   * the Document's lastModified, size, and creation time will be that of this FileReference (if specified).
   * The input stream will be used to get the file's content, if {@link TraversalParams#shouldGetFileContent()} is true.
   * @throws IOException If an error occurs reading the given InputStream.
   */
  Document decompressedFileAsDoc(InputStream in, String decompressedFullPathStr, TraversalParams params) throws IOException;
}
