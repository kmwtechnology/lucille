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
 * <p> Call the {@link #markFileOrDirectoryEncountered(String)} method <b>for every file and directory</b> you encounter during a traversal - regardless of
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

  // tracking the files (and the times they were published) and the directories we knew about / encountered
  private final Map<String, Instant> fileStateEntries;
  private final Set<String> directories;

  private final Set<String> pathsToDelete;

  // Using a single Instant to apply to all files encountered as part of this traversal.
  private final Instant traversalInstant;

  /**
   * Creates a StorageClientState, representing and managing the given Map of file paths to Instants at which they were last modified,
   * read from a state database. This object will track each of these file paths, and when the database is updated to reflect this state,
   * any file paths that were not referenced in a call to {@link #markFileOrDirectoryEncountered(String)} will be removed from the database.
   *
   * <p> <b>Note:</b> This constructor will <b>not</b> mutate the given Set / Map - it deep copies their contents.
   * @param fileStateEntries A map of file paths (Strings) to the Instant at which they were last published by Lucille, according
   *                     to the state database.
   */
  public StorageClientState(Set<String> directories, Map<String, Instant> fileStateEntries) {
    this.fileStateEntries = new HashMap<>(fileStateEntries);
    this.directories = new HashSet<>(directories);

    // The paths that we haven't encountered in our traversal. As we see them, we will remove them from the set.
    // At the end of the traversal, these paths will be removed from the directory.
    this.pathsToDelete = new HashSet<>();
    pathsToDelete.addAll(fileStateEntries.keySet());
    pathsToDelete.addAll(directories);

    this.traversalInstant = Instant.now();
  }

  /**
   * Update this state to reflect that the given file was encountered during a StorageClient traversal.
   * @param fullPathStr The full path to the file you encountered during a StorageClient traversal.
   */
  public void markFileOrDirectoryEncountered(String fullPathStr) {
    if (fullPathStr.endsWith("/") && !directories.contains(fullPathStr)) {
      directories.add(fullPathStr);
      log.info("New {} found by StorageClient, added to directories.", fullPathStr);
    }

    boolean removed = pathsToDelete.remove(fullPathStr);

    if (removed) {
      log.debug("Known file/directory {} was encountered by StorageClient.", fullPathStr);
    } else {
      log.debug("New file/directory {} was encountered by StorageClient.", fullPathStr);
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
    return fileStateEntries.get(fullPathStr);
  }

  /**
   * Updates this state to reflect that the given file was successfully published during a StorageClient traversal.
   * @param fullPathStr The full path to the file that was successfully published.
   */
  public void successfullyPublishedFile(String fullPathStr) {
    if (pathsToDelete.contains(fullPathStr)) {
      log.warn("{} was marked as published, but not encountered", fullPathStr);
    }

    fileStateEntries.put(fullPathStr, traversalInstant);
  }

  // Package access getters
  /**
   * Returns the Map of file paths to Instants at which they were last published, held by this State. Package access, as this should
   * only be accessed in {@link StorageClientStateManager} and some unit tests.
   * @return the Map of file paths to Instants at which they were last published, held by this State.
   */
  Map<String, Instant> getFileStateEntries() {
    return fileStateEntries;
  }

  /**
   * Returns the Set of file paths which were found in the initially provided {@link #fileStateEntries}, but have not had a call to
   * {@link #markFileOrDirectoryEncountered(String)} yet.
   * <p> Package access, as this should only be accessed in {@link StorageClientStateManager} and some unit tests.
   * @return the Set of file / directory paths that were not encountered (presumably, deleted/moved/renamed)
   * and should be removed from the database.
   */
  Set<String> getPathsToDelete() {
    return pathsToDelete;
  }

  /**
   * Returns the Set of file paths which were found in the initially provided {@link #fileStateEntries}, but have not had a call to
   * {@link #markFileOrDirectoryEncountered(String)} yet.
   * <p> Package access, as this should only be accessed in {@link StorageClientStateManager} and some unit tests.
   * @return the Set of directories that either previously had
   */
  Set<String> getDirectories() {
    return directories;
  }
}
