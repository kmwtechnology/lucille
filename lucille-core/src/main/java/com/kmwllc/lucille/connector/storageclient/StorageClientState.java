package com.kmwllc.lucille.connector.storageclient;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds state regarding the relevant files in a StorageClient traversal, namely, files and the last time they were processed &
 * published by Lucille.
 *
 * <p> Call the encounteredFile(FileReference, TraversalParams) method <b>for every file and directory</b> you encounter during a traversal - regardless of
 * whether it complies with FilterOptions or not.
 * <p> Call the getLastPublished(FileReference, TraversalParams) method to lookup when the file was last published by Lucille.
 * Returns null if there is no record of the file being processed by Lucille.
 */
public class StorageClientState {

  private static final Logger log = LoggerFactory.getLogger(StorageClientState.class);

  private final Map<String, Instant> stateEntries;
  private final Set<String> unencounteredPaths;
  // Using a single Instant to apply to all files encountered as part of this traversal.
  private final Instant traversalInstant;

  public StorageClientState(Map<String, Instant> stateEntries) {
    this.stateEntries = stateEntries;

    // stateEntries.keySet() is not a deep copy of the entries - changes to one cause changes in the other.
    this.unencounteredPaths = new HashSet<>();
    for (String statePath : stateEntries.keySet()) {
      unencounteredPaths.add(statePath);
    }

    this.traversalInstant = Instant.now();
  }

  /**
   *
   * @param fullPathStr
   */
  public void encounteredFile(String fullPathStr) {
    boolean removed = unencounteredPaths.remove(fullPathStr);

    if (removed) {
      log.debug("Known file {} was encountered by StorageClient.", fullPathStr);
    } else {
      log.debug("New file {} was encountered by StorageClient.", fullPathStr);
    }
  }

  public Instant getLastPublished(FileReference fileReference) {
    String filePath = fileReference.getFullPath();
    return stateEntries.get(filePath);
  }

  public void publishedFile(String fullPathStr) {
    stateEntries.put(fullPathStr, traversalInstant);
  }
}
