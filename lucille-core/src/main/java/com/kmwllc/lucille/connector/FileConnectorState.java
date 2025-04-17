package com.kmwllc.lucille.connector;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds state regarding the relevant files in a FileConnector traversal, namely, files and the last time they were processed &
 * published by Lucille.
 *
 * <p> Call the {@link #markFileOrDirectoryEncountered(String)} method <b>for every file and directory</b> you encounter during a traversal - regardless of
 * whether it complies with FilterOptions or not.
 * <p> Call the {@link #successfullyPublishedFile(String)} method after a file was successfully published by Lucille.
 * <p> Call the {@link #getLastPublished(String)} method to lookup when the file was last published by Lucille.
 * Returns null if there is no record of the file being processed by Lucille.
 *
 * <p> When your traversal is complete, you'll call {@link FileConnectorStateManager#updateStateDatabase(FileConnectorState, String)} with this
 * state to update your database and reflect the results of your traversal.
 */
public class FileConnectorState {

  private static final Logger log = LoggerFactory.getLogger(FileConnectorState.class);

  // Based on the initial information provided in the constructor (the previous state read / built from the database).
  private final Set<String> knownDirectories;
  private final Map<String, Instant> knownFileStateEntries;

  // These grow as markFileOrDirectoryEncountered / successfullyPublishedFile are called.
  private final Set<String> encounteredDirectories;
  // all paths to files that were encountered (but not necessarily published!) in a traversal
  private final Set<String> encounteredFiles;
  private final Set<String> publishedFiles;

  /**
   * Creates a FileConnectorState, representing and managing the given Map of file paths to Instants at which they were last modified,
   * read from a state database. This object will track each of these file paths, and when the database is updated to reflect this state,
   * any file paths that were not referenced in a call to {@link #markFileOrDirectoryEncountered(String)} will be removed from the database.
   *
   * <p> <b>Note:</b> This constructor will <b>not</b> mutate the given Set / Map - it deep copies their contents.
   * @param knownDirectories The directories which the state database has entries for.
   * @param knownFileStateEntries A map of file paths (Strings) to the Instant at which they were last published by Lucille, according
   *                     to the state database.
   */
  public FileConnectorState(Set<String> knownDirectories, Map<String, Instant> knownFileStateEntries) {
    // The paths that we haven't encountered in our traversal. As we see them, we will remove them from the set.
    // At the end of the traversal, these paths will be removed from the directory.
    this.knownDirectories = new HashSet<>(knownDirectories);
    this.knownFileStateEntries = new HashMap<>(knownFileStateEntries);

    this.encounteredDirectories = new HashSet<>();
    this.encounteredFiles = new HashSet<>();
    this.publishedFiles = new HashSet<>();
  }

  /**
   * Update this state to reflect that the given file or directory was encountered during a FileConnector traversal. Directories
   * must end with the path separator.
   * @param fullPathStr The full path to the file or directory you encountered during a FileConnector traversal. Directories must
   *                    end with the path separator.
   */
  public void markFileOrDirectoryEncountered(String fullPathStr) {
    if (fullPathStr.endsWith("/")) {
      encounteredDirectories.add(fullPathStr);
    } else {
      encounteredFiles.add(fullPathStr);
    }
  }

  /**
   * Retrieves the instant at which this file was last known to be published by Lucille. If this FileConnectorState has
   * no record of publishing this file, a null Instant is returned. {@link #successfullyPublishedFile(String)} has no
   * effect on the return value of this method - it is entirely dependent on the information previously retrieved
   * from the database.
   *
   * @param fullPathStr The full path to the file you want to get this information for.
   * @return The instant at which this file was last known to be published by Lucille; null if there is no information
   * on this file.
   */
  public Instant getLastPublished(String fullPathStr) {
    return knownFileStateEntries.get(fullPathStr);
  }

  /**
   * Updates this state to reflect that the given file was successfully published during a FileConnector traversal.
   * @param fullPathStr The full path to the file that was successfully published.
   */
  public void successfullyPublishedFile(String fullPathStr) {
    if (!encounteredFiles.contains(fullPathStr)) {
      log.warn("{} was marked as published, but not encountered - it may get deleted!", fullPathStr);
    }

    publishedFiles.add(fullPathStr);
  }

  // Getters / methods for FileConnectorStateManager to write / modify necessary information in database

  /**
   * Returns all the files that were previously represented / found in the State database and have been published.
   * These are paths in need of an "UPDATE" on the state database, changing just their timestamp.
   */
  public Set<String> getKnownAndPublishedFilePaths() {
    Set<String> knownAndPublishedFilePaths = new HashSet<>();

    for (String encounteredFile : encounteredFiles) {
      // previously had state for the file, and it was published --> should get an UPDATE
      if (knownFileStateEntries.containsKey(encounteredFile) && publishedFiles.contains(encounteredFile)) {
        knownAndPublishedFilePaths.add(encounteredFile);
      }
    }

    return knownAndPublishedFilePaths;
  }

  /**
   * Returns all the files that were not previously represented / found in the State database and have been published.
   * These are paths in need of an "INSERT" on the state database.
   */
  public Set<String> getNewlyPublishedFilePaths() {
    Set<String> newFilePaths = new HashSet<>();

    for (String encounteredFile : encounteredFiles) {
      // didn't previously have state for the file, and it was published --> should get an "INSERT"
      if (!knownFileStateEntries.containsKey(encounteredFile) && publishedFiles.contains(encounteredFile)) {
        newFilePaths.add(encounteredFile);
      }
    }

    return newFilePaths;
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

    Set<String> filesNotEncountered = new HashSet<>(knownFileStateEntries.keySet());
    filesNotEncountered.removeAll(encounteredFiles);

    results.addAll(directoriesNotEncountered);
    results.addAll(filesNotEncountered);
    return results;
  }
}
