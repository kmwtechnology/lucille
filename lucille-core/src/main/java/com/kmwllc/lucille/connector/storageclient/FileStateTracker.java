package com.kmwllc.lucille.connector.storageclient;

import java.time.Instant;

/**
 * The state a StorageClient reads and updates during an incremental traversal, keyed by the full path of each file.
 * FileConnector passes one to {@link StorageClient#traverse(com.kmwllc.lucille.core.Publisher, TraversalParams, FileStateTracker)}
 * when state is enabled, and is responsible for its lifecycle; a StorageClient only calls the per-file methods below.
 *
 * <p> The key for a file is <code>getFullPath().toString()</code> of its {@link FileReference}. For an entry inside an
 * archive or compressed file, the key is the container's full path, then {@link StorageClientFields#ARCHIVE_FILE_SEPARATOR},
 * then the entry's name. Keys must be stable across runs: a file whose key changes is treated as a new file, and the old
 * key as a deleted one.
 *
 * <p> FileConnector's implementation is backed by a JDBC database. Tests can supply a simple in-memory implementation.
 * Methods are called from the thread doing the traversal.
 */
public interface FileStateTracker {

  /**
   * Records that the file with the given key was seen during this traversal, whether or not it is published. Files that
   * are never marked are treated as deleted.
   * @param fullPathStr The key for the file.
   */
  void markFileEncountered(String fullPathStr);

  /**
   * Records that every file whose key starts with the given prefix was seen during this traversal. Used for the entries
   * of an archive or compressed file that is not re-read this run, so that its entries are not treated as deleted.
   * @param prefix A key prefix, typically the container's full path followed by {@link StorageClientFields#ARCHIVE_FILE_SEPARATOR}.
   */
  void markAllEntriesEncountered(String prefix);

  /**
   * Returns when the file with the given key was last published, or null if it has not been.
   * @param fullPathStr The key for the file.
   * @return The instant the file was last published, or null if it has no record of being published.
   */
  Instant getLastPublished(String fullPathStr);

  /**
   * Records that the file with the given key was published (or otherwise fully processed) during this traversal.
   * @param fullPathStr The key for the file.
   */
  void successfullyPublishedFile(String fullPathStr);
}
