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

  private final Set<String> knownDirectories;
  // Is based on the initial information provided in the constructor. fileStateEntries will grow, but we can still check which
  // entries it has that aren't in encounteredFiles to check what we need to remove.
  private final Map<String, Instant> fileStateEntries;

  // These are sets that grow as markFileOrDirectoryEncountered is called. Supports determining which paths in the database
  // need to be deleted.
  private final Set<String> encounteredDirectories;
  private final Set<String> encounteredFiles;

  // Using a single Instant to apply to all files encountered as part of this traversal.
  private final Instant traversalInstant;

  /**
   * Creates a StorageClientState, representing and managing the given Map of file paths to Instants at which they were last modified,
   * read from a state database. This object will track each of these file paths, and when the database is updated to reflect this state,
   * any file paths that were not referenced in a call to {@link #markFileOrDirectoryEncountered(String)} will be removed from the database.
   *
   * <p> <b>Note:</b> This constructor will <b>not</b> mutate the given Set / Map - it deep copies their contents.
   * @param knownDirectories The directories which the state database has entries for.
   * @param fileStateEntries A map of file paths (Strings) to the Instant at which they were last published by Lucille, according
   *                     to the state database.
   */
  public StorageClientState(Set<String> knownDirectories, Map<String, Instant> fileStateEntries) {
    // The paths that we haven't encountered in our traversal. As we see them, we will remove them from the set.
    // At the end of the traversal, these paths will be removed from the directory.
    this.knownDirectories = new HashSet<>(knownDirectories);
    this.fileStateEntries = new HashMap<>(fileStateEntries);

    this.encounteredDirectories = new HashSet<>();
    this.encounteredFiles = new HashSet<>();

    this.traversalInstant = Instant.now();
  }

  /**
   * Update this state to reflect that the given file or directory was encountered during a StorageClient traversal. Directories
   * must end with the path separator.
   * @param fullPathStr The full path to the file or directory you encountered during a StorageClient traversal. Directories must
   *                    end with the path separator.
   */
  public void markFileOrDirectoryEncountered(String fullPathStr) {
    if (fullPathStr.endsWith("/")) {
      log.debug("Directory {} found by StorageClient, added to encounteredDirectories.", fullPathStr);
      encounteredDirectories.add(fullPathStr);
    } else {
      log.debug("File {} found by StorageClient, added to encounteredDirectories.", fullPathStr);
      encounteredFiles.add(fullPathStr);
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
    if (!encounteredFiles.contains(fullPathStr)) {
      log.warn("{} was marked as published, but not encountered - it may get deleted!", fullPathStr);
    }

    fileStateEntries.put(fullPathStr, traversalInstant);
  }

  // Getters / methods for StorageClientStateManager to write / modify necessary information in database

  /**
   * Based on the directories and files that have been marked as encountered by {@link #markFileOrDirectoryEncountered(String)}, and
   * the known directories and files provided at construction, returns a Set of file / directory paths that have presumably been
   * moved / deleted and should be removed from the state database.
   * @return A Set of file / directory paths that should be removed from the state database.
   */
  public Set<String> getPathsToDelete() {
    Set<String> results = new HashSet<>();

    Set<String> directoriesNotEncountered = new HashSet<>(knownDirectories);
    directoriesNotEncountered.removeAll(encounteredDirectories);

    Set<String> filesNotEncountered = new HashSet<>(fileStateEntries.keySet());
    filesNotEncountered.removeAll(encounteredFiles);

    results.addAll(directoriesNotEncountered);
    results.addAll(filesNotEncountered);
    return results;
  }

  /**
   * Returns the Map of file paths to Instants at which they were last published, held & modified by this State. This Map may
   * contain file paths that should be deleted (per {@link #getPathsToDelete()}).
   */
  public Map<String, Instant> getEncounteredFileStateEntries() {
    Map<String, Instant> encounteredFileEntries = new HashMap<>();

    for (String encounteredFile : encounteredFiles) {
      encounteredFileEntries.put(encounteredFile, fileStateEntries.get(encounteredFile));
    }

    return encounteredFileEntries;
  }

  /**
   * Returns a Set of String paths to the new directories found in the traversal.
   * @return a Set of String paths to the new directories found in the traversal.
   */
  public Set<String> getNewDirectoryPaths() {
    Set<String> newDirectoryPaths = new HashSet<>(encounteredDirectories);
    newDirectoryPaths.removeAll(knownDirectories);

    return newDirectoryPaths;
  }
}
