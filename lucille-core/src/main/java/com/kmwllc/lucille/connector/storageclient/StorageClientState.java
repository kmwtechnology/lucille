package com.kmwllc.lucille.connector.storageclient;

import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds state regarding the relevant files in a StorageClient traversal, namely, files and the last time they were processed &
 * published by Lucille.
 *
 * <p> Call the {@link #encounteredFile(String)} method <b>for every file and directory</b> you encounter during a traversal - regardless of
 * whether it complies with FilterOptions or not.
 * <p> Call the {@link #getLastPublished(String)} method to lookup when the file was last published by Lucille.
 * Returns null if there is no record of the file being processed by Lucille.
 * <p> Call the {@link #successfullyPublishedFile(String)} method after a file was successfully published by Lucille.
 *
 * <p> When your traversal is complete, you'll call {@link StorageClientStateManager#updateState(StorageClientState)} with this
 * state to update your database and reflect the results of your traversal.
 * <p> <b>Note:</b> All calls to {@link #successfullyPublishedFile(String)} will result in the file paths having the same Instant
 * in the updated State. This Instant is created when this StorageClientState is constructed.
 */
public class StorageClientState {

  private static final Logger log = LoggerFactory.getLogger(StorageClientState.class);

  private final Map<String, Instant> stateEntries;
  private final Set<String> notEncounteredPaths;
  // Using a single Instant to apply to all files encountered as part of this traversal.
  private final Instant traversalInstant;

  /**
   * Creates a StorageClientState, representing and managing the given Map of file paths to Instants at which they were last modified,
   * read from a state database. This object will track each of these file paths, and when the database is updated to reflect this state,
   * any file paths that were not referenced in a call to {@link #encounteredFile(String)} will be removed from the database.
   * @param stateEntries A map of file paths (Strings) to the Instant at which they were last published by Lucille, according
   *                     to the state database.
   */
  public StorageClientState(Map<String, Instant> stateEntries) {
    this.stateEntries = stateEntries;

    // stateEntries.keySet() is not a deep copy of the entries - changes to one cause changes in the other.
    this.notEncounteredPaths = new HashSet<>();
    notEncounteredPaths.addAll(stateEntries.keySet());

    this.traversalInstant = Instant.now();
  }

  /**
   * Update this state to reflect that the given file was encountered during a StorageClient traversal.
   * @param fullPathStr The full path to the file you encountered during a StorageClient traversal.
   */
  public void encounteredFile(String fullPathStr) {
    boolean removed = notEncounteredPaths.remove(fullPathStr);

    if (removed) {
      log.debug("Known file {} was encountered by StorageClient.", fullPathStr);
    } else {
      log.debug("New file {} was encountered by StorageClient.", fullPathStr);
    }
  }

  // TODO: What is the behavior for archive files and their entries?

  /**
   * Retrieves the instant at which this file was last known to be published by Lucille. If this StorageClientState has
   * no record of publishing this file, a null Instant is returned.
   * @param fullPathStr The full path to the file you want to get this information for.
   * @return The instant at which this file was last known to be published by Lucille; null if there is no information
   * on this file.
   */
  public Instant getLastPublished(String fullPathStr) {
    return stateEntries.get(fullPathStr);
  }

  /**
   * Updates this state to reflect that the given file was successfully published during a StorageClient traversal.
   * @param fullPathStr The full path to the file that was successfully published.
   */
  public void successfullyPublishedFile(String fullPathStr) {
    if (notEncounteredPaths.contains(fullPathStr)) {
      log.warn("{} was marked as published, but not encountered", fullPathStr);
    }

    stateEntries.put(fullPathStr, traversalInstant);
  }

  // Package access getters
  /**
   * Returns the Map of file paths to Instants at which they were last published, held by this State. Package access, as this should
   * only be accessed in {@link StorageClientStateManager} and some unit tests.
   * @return the Map of file paths to Instants at which they were last published, held by this State.
   */
  Map<String, Instant> getStateEntries() {
    return stateEntries;
  }

  /**
   * Returns the Set of file paths which were found in the initially provided {@link #stateEntries}, but have not has a call to
   * {@link #encounteredFile(String)} yet. Package access, as this should only be accessed in {@link StorageClientStateManager}
   * and some unit tests.
   * @return the Set of not encountered file paths.
   */
  Set<String> getNotEncounteredPaths() {
    return notEncounteredPaths;
  }
}
