package com.kmwllc.lucille.connector.storageclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class StorageClientStateTest {

  private static final String helloFile = "/hello.txt";
  private static final String infoFile = "/files/info.txt";
  private static final String secretsFile = "/files/subdir/secrets.txt";

  private static final String filesDirectory = "/files/";
  private static final String subdirDirectory = "/files/subdir/";

  // StorageClientState will not mutate these.
  private static final Set<String> allDirectories = Set.of(filesDirectory, subdirDirectory);

  private static final Map<String, Instant> allFiles = Map.of(
      helloFile, Instant.ofEpochMilli(1),
      infoFile, Instant.ofEpochMilli(2),
      secretsFile, Instant.ofEpochMilli(3));

  private static final Map<String, Instant> allFilesNoSecrets = Map.of(
      helloFile, Instant.ofEpochMilli(1),
      infoFile, Instant.ofEpochMilli(2));

  // An example of traversing with existing state entries for each file.
  @Test
  public void testExampleTraversal() {
    // Data that would be read from the database + assembled by StorageClientStateManager
    StorageClientState state = new StorageClientState(allDirectories, allFiles);

    // we encounter all files and directories, and publish every file.
    state.markFileOrDirectoryEncountered(filesDirectory);
    state.markFileOrDirectoryEncountered(subdirDirectory);

    state.markFileOrDirectoryEncountered(helloFile);
    state.successfullyPublishedFile(helloFile);

    state.markFileOrDirectoryEncountered(infoFile);
    state.successfullyPublishedFile(infoFile);

    state.markFileOrDirectoryEncountered(secretsFile);
    state.successfullyPublishedFile(secretsFile);

    // There shouldn't be anything to delete nor new directories we encountered.
    assertTrue(state.getPathsToDelete().isEmpty());
    assertTrue(state.getNewDirectoryPaths().isEmpty());

    // There should be three entries for encountered files. No new file paths.
    assertEquals(3, state.getKnownAndPublishedFilePaths().size());
    assertEquals(0, state.getNewlyPublishedFilePaths().size());
  }

  // An example of traversing where we find a "new file", one we didn't have a state entry for.
  @Test
  public void testExampleTraversalWithNewFiles() {
    StorageClientState state = new StorageClientState(allDirectories, allFilesNoSecrets);

    // we encounter and publish every file / directory.
    // State did not previously have record on the secretsFile.
    state.markFileOrDirectoryEncountered(filesDirectory);
    state.markFileOrDirectoryEncountered(subdirDirectory);

    state.markFileOrDirectoryEncountered(helloFile);
    state.successfullyPublishedFile(helloFile);

    state.markFileOrDirectoryEncountered(infoFile);
    state.successfullyPublishedFile(infoFile);

    state.markFileOrDirectoryEncountered(secretsFile);
    state.successfullyPublishedFile(secretsFile);

    // There shouldn't be anything to delete nor new *directories* encountered.
    assertTrue(state.getPathsToDelete().isEmpty());
    assertTrue(state.getNewDirectoryPaths().isEmpty());

    // There should be two known and published files, one newly published file (secrets).
    assertEquals(2, state.getKnownAndPublishedFilePaths().size());
    assertEquals(1, state.getNewlyPublishedFilePaths().size());
  }

  // An example of a traversal where we don't encounter a file that we have a state entry for,
  // indicating it was deleted, renamed, or moved.
  @Test
  public void testExampleTraversalWithDeletions() {
    StorageClientState state = new StorageClientState(allDirectories, allFiles);

    // we encounter each file / directory, and publish each file, but nothing happens for secrets (not found).
    state.markFileOrDirectoryEncountered(filesDirectory);
    state.markFileOrDirectoryEncountered(subdirDirectory);

    state.markFileOrDirectoryEncountered(helloFile);
    state.successfullyPublishedFile(helloFile);

    state.markFileOrDirectoryEncountered(infoFile);
    state.successfullyPublishedFile(infoFile);

    // we will delete secretsFile as we didn't encounter it.
    assertTrue(state.getPathsToDelete().contains(secretsFile));

    // We have updates for hello and info files, nothing was new
    assertEquals(2, state.getKnownAndPublishedFilePaths().size());
    assertEquals(0, state.getNewlyPublishedFilePaths().size());
  }

  // An example of a traversal where publishing on a file doesn't take place - either because it
  // didn't meet filter options, an error occurred, etc.
  @Test
  public void testExampleTraversalWithoutSomePublishing() {
    StorageClientState state = new StorageClientState(allDirectories, allFilesNoSecrets);

    // we encounter each file / directory, and publish every file except for secrets.
    state.markFileOrDirectoryEncountered(filesDirectory);
    state.markFileOrDirectoryEncountered(subdirDirectory);

    state.markFileOrDirectoryEncountered(helloFile);
    state.successfullyPublishedFile(helloFile);

    // info is known, and encountered, but not published. it won't be deleted, but won't get an "UPDATE".
    state.markFileOrDirectoryEncountered(infoFile);

    // secrets is NEWLY encountered, but not published. it won't get an "INSERT".
    state.markFileOrDirectoryEncountered(secretsFile);

    // There shouldn't be any files/directories we previously had state for that we didn't encounter in
    // our traversal.
    assertEquals(0, state.getPathsToDelete().size());
    assertEquals(0, state.getNewDirectoryPaths().size());

    // Only "hello" file was known and was published, so it will get an update
    assertEquals(1, state.getKnownAndPublishedFilePaths().size());
    // No insert for "secretsFile" because, though it was new, it wasn't published.
    assertEquals(0, state.getNewlyPublishedFilePaths().size());
  }

  // An example of a traversal where a directory is deleted (in this example, we say subdir was deleted.)
  // The previous state still has entries for all files and directories.
  @Test
  public void testExampleTraversalWithDeletedDirectory() {
    StorageClientState state = new StorageClientState(allDirectories, allFiles);

    // we only encounter files / directories that aren't in/are subdir.
    state.markFileOrDirectoryEncountered(filesDirectory);

    state.markFileOrDirectoryEncountered(helloFile);
    state.successfullyPublishedFile(helloFile);

    state.markFileOrDirectoryEncountered(infoFile);
    state.successfullyPublishedFile(infoFile);

    // delete both the subdir directory and the secrets file.
    assertEquals(2, state.getPathsToDelete().size());
    assertTrue(state.getPathsToDelete().contains(subdirDirectory));
    assertTrue(state.getPathsToDelete().contains(secretsFile));
    // no new directories
    assertEquals(0, state.getNewDirectoryPaths().size());

    // entries for the two files we do encounter + publish
    assertEquals(2, state.getKnownAndPublishedFilePaths().size());
    assertEquals(0, state.getNewlyPublishedFilePaths().size());
  }

  // Pretending we haven't encountered subdir or subdir/secrets.txt before
  @Test
  public void testExampleTraversalWithNewDirectory() {
    StorageClientState state = new StorageClientState(Set.of(filesDirectory), allFilesNoSecrets);

    // we encounter and publish every file / directory.
    // State did not previously have record on the subdir directory or the secretsFile.
    state.markFileOrDirectoryEncountered(filesDirectory);
    state.markFileOrDirectoryEncountered(subdirDirectory);

    state.markFileOrDirectoryEncountered(helloFile);
    state.successfullyPublishedFile(helloFile);

    state.markFileOrDirectoryEncountered(infoFile);
    state.successfullyPublishedFile(infoFile);

    state.markFileOrDirectoryEncountered(secretsFile);
    state.successfullyPublishedFile(secretsFile);

    assertEquals(0, state.getPathsToDelete().size());
    // encountered one new directory, the subdirDirectory.
    assertEquals(1, state.getNewDirectoryPaths().size());
    assertTrue(state.getNewDirectoryPaths().contains(subdirDirectory));

    // knew about two files, have one new file
    assertEquals(2, state.getKnownAndPublishedFilePaths().size());
    assertEquals(1, state.getNewlyPublishedFilePaths().size());
  }
}
