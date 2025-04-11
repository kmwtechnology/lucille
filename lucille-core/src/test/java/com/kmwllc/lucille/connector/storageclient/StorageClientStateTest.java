package com.kmwllc.lucille.connector.storageclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class StorageClientStateTest {

  private final String helloFile = "/hello.txt";
  private final String infoFile = "/files/info.txt";
  private final String secretsFile = "/files/subdir/secrets.txt";

  private final String filesDirectory = "/files/";
  private final String subdirDirectory = "/files/subdir/";

  // StorageClientState will not mutate these.
  private final Set<String> allDirectories = Set.of(filesDirectory, subdirDirectory);

  private final Map<String, Instant> allFiles = Map.of(
      helloFile, Instant.ofEpochMilli(1),
      infoFile, Instant.ofEpochMilli(2),
      secretsFile, Instant.ofEpochMilli(3));

  private final Map<String, Instant> allFilesNoSecrets = Map.of(
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

    // want to make sure that the entries have been updated accordingly, leveraging that
    // each file encountered will have the same Instant for their "last published time".
    assertEquals(state.getLastPublished(helloFile), state.getLastPublished(infoFile));
    assertEquals(state.getLastPublished(helloFile), state.getLastPublished(secretsFile));

    // There shouldn't be anything to delete nor new directories we encountered.
    assertTrue(state.getPathsToDelete().isEmpty());
    assertTrue(state.getNewDirectoryPaths().isEmpty());

    // There should be three entries for encountered files.
    assertEquals(3, state.getEncounteredFileStateEntries().size());
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

    assertNotEquals(Instant.ofEpochMilli(1), state.getLastPublished(helloFile));
    assertNotEquals(Instant.ofEpochMilli(2), state.getLastPublished(infoFile));

    // again, each file encountered will have the same Instant as their "last published time".
    // even though "secretsFile" wasn't already in the state entries.
    assertEquals(state.getLastPublished(helloFile), state.getLastPublished(infoFile));
    assertEquals(state.getLastPublished(helloFile), state.getLastPublished(secretsFile));

    // There shouldn't be anything to delete nor new *directories* encountered.
    assertTrue(state.getPathsToDelete().isEmpty());
    assertTrue(state.getNewDirectoryPaths().isEmpty());

    // There should be three entries for encountered files.
    assertEquals(3, state.getEncounteredFileStateEntries().size());
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

    // These two files should have the updated "published" time.
    assertEquals(state.getLastPublished(helloFile), state.getLastPublished(infoFile));

    // we will delete secretsFile as we didn't encounter it.
    assertTrue(state.getPathsToDelete().contains(secretsFile));
    // the method should only return state entries for the encountered files, even though we supplied
    // it entries for all files in the constructor!
    assertEquals(2, state.getEncounteredFileStateEntries().size());
  }

  // An example of a traversal where publishing on a file doesn't take place - either because it
  // didn't meet filter options, an error occurred, etc.
  @Test
  public void testExampleTraversalWithoutSomePublishing() {
    StorageClientState state = new StorageClientState(allDirectories, allFiles);

    // we encounter each file / directory, and publish every file except for secrets.
    state.markFileOrDirectoryEncountered(filesDirectory);
    state.markFileOrDirectoryEncountered(subdirDirectory);

    state.markFileOrDirectoryEncountered(helloFile);
    state.successfullyPublishedFile(helloFile);

    state.markFileOrDirectoryEncountered(infoFile);
    state.successfullyPublishedFile(infoFile);

    // secrets is encountered - meaning it won't be deleted from the database! - but not published,
    // so the time is not updated.
    state.markFileOrDirectoryEncountered(secretsFile);

    // These two files should have the updated "published" time.
    assertEquals(state.getLastPublished(helloFile), state.getLastPublished(infoFile));
    // secrets should still have the old one.
    assertEquals(Instant.ofEpochMilli(3), state.getLastPublished(secretsFile));

    // There shouldn't be any files/directories we previously had state for that we didn't encounter in
    // our traversal.
    assertTrue(state.getPathsToDelete().isEmpty());
    // Still return a state entry for all the files, even though secret wasn't published.
    assertEquals(3, state.getEncounteredFileStateEntries().size());
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

    // These two files should have the updated "published" time.
    assertEquals(state.getLastPublished(helloFile), state.getLastPublished(infoFile));

    // delete both the subdir directory and the secrets file.
    assertEquals(2, state.getPathsToDelete().size());
    assertTrue(state.getPathsToDelete().contains(subdirDirectory));
    assertTrue(state.getPathsToDelete().contains(secretsFile));

    // entries for the two files we do encounter.
    assertEquals(2, state.getEncounteredFileStateEntries().size());
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

    assertEquals(state.getLastPublished(helloFile), state.getLastPublished(infoFile));
    assertEquals(state.getLastPublished(helloFile), state.getLastPublished(secretsFile));

    assertTrue(state.getPathsToDelete().isEmpty());

    // encountered one new directory, the subdirDirectory.
    assertEquals(1, state.getNewDirectoryPaths().size());
    assertTrue(state.getNewDirectoryPaths().contains(subdirDirectory));

    // have entries for three files
    assertEquals(3, state.getEncounteredFileStateEntries().size());
  }
}
