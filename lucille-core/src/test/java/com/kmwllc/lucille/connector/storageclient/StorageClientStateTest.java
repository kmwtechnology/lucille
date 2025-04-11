package com.kmwllc.lucille.connector.storageclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class StorageClientStateTest {

  private final String helloFile = "/hello.txt";
  private final String infoFile = "/files/info.txt";
  private final String secretsFile = "/files/subdir/secrets.txt";

  // An example of traversing with existing state entries for each file.
  @Test
  public void testExampleTraversal() {
    // Data that would be read from the database + assembled by StorageClientStateManager
    // TODO: Needs to include directories?
    Map<String, Instant> exampleStateEntries = new HashMap<>();
    exampleStateEntries.put(helloFile, Instant.ofEpochMilli(1));
    exampleStateEntries.put(infoFile, Instant.ofEpochMilli(2));
    exampleStateEntries.put(secretsFile, Instant.ofEpochMilli(3));

    StorageClientState state = new StorageClientState(exampleStateEntries);

    // we encounter and publish every file.
    state.encounteredFile(helloFile);
    state.successfullyPublishedFile(helloFile);

    state.encounteredFile(infoFile);
    state.successfullyPublishedFile(infoFile);

    state.encounteredFile(secretsFile);
    state.successfullyPublishedFile(secretsFile);

    // want to make sure that the entries have been updated accordingly
    assertNotEquals(Instant.ofEpochMilli(1), state.getLastPublished(helloFile));
    assertNotEquals(Instant.ofEpochMilli(2), state.getLastPublished(infoFile));
    assertNotEquals(Instant.ofEpochMilli(3), state.getLastPublished(secretsFile));

    // minor detail - each file encountered will have the same Instant as their "last published time".
    assertEquals(state.getLastPublished(helloFile), state.getLastPublished(infoFile));
    assertEquals(state.getLastPublished(helloFile), state.getLastPublished(secretsFile));
  }

  // An example of traversing where we find a "new file", one we didn't have a state entry for.
  @Test
  public void testExampleTraversalWithNewFiles() {
    Map<String, Instant> exampleStateEntries = new HashMap<>();
    exampleStateEntries.put(helloFile, Instant.ofEpochMilli(1));
    exampleStateEntries.put(infoFile, Instant.ofEpochMilli(2));

    StorageClientState state = new StorageClientState(exampleStateEntries);

    // we encounter and publish every file. State did not previously have record on the secretsFile.
    state.encounteredFile(helloFile);
    state.successfullyPublishedFile(helloFile);

    state.encounteredFile(infoFile);
    state.successfullyPublishedFile(infoFile);

    state.encounteredFile(secretsFile);
    state.successfullyPublishedFile(secretsFile);

    assertNotEquals(Instant.ofEpochMilli(1), state.getLastPublished(helloFile));
    assertNotEquals(Instant.ofEpochMilli(2), state.getLastPublished(infoFile));

    // again, each file encountered will have the same Instant as their "last published time".
    // even though "secretsFile" wasn't already in the state entries.
    assertEquals(state.getLastPublished(helloFile), state.getLastPublished(infoFile));
    assertEquals(state.getLastPublished(helloFile), state.getLastPublished(secretsFile));
  }

  // An example of a traversal where we don't encounter a file that we have a state entry for,
  // indicating it was deleted, renamed, or moved.
  @Test
  public void testExampleTraversalWithDeletions() {
    Map<String, Instant> exampleStateEntries = new HashMap<>();
    exampleStateEntries.put(helloFile, Instant.ofEpochMilli(1));
    exampleStateEntries.put(infoFile, Instant.ofEpochMilli(2));
    exampleStateEntries.put(secretsFile, Instant.ofEpochMilli(3));

    StorageClientState state = new StorageClientState(exampleStateEntries);

    // we encounter and publish each file, except for secrets.
    state.encounteredFile(helloFile);
    state.successfullyPublishedFile(helloFile);

    state.encounteredFile(infoFile);
    state.successfullyPublishedFile(infoFile);

    // These two files should have the updated "published" time.
    assertEquals(state.getLastPublished(helloFile), state.getLastPublished(infoFile));

    assertTrue(state.getNotEncounteredPaths().contains(secretsFile));
  }

  // An example of a traversal where publishing on a file doesn't take place - either because it
  // didn't meet filter options, an error occurred, etc.
  @Test
  public void testExampleTraversalWithoutSomePublishing() {
    Map<String, Instant> exampleStateEntries = new HashMap<>();
    exampleStateEntries.put(helloFile, Instant.ofEpochMilli(1));
    exampleStateEntries.put(infoFile, Instant.ofEpochMilli(2));
    exampleStateEntries.put(secretsFile, Instant.ofEpochMilli(3));

    StorageClientState state = new StorageClientState(exampleStateEntries);

    // we encounter and publish each file, except for secrets.
    state.encounteredFile(helloFile);
    state.successfullyPublishedFile(helloFile);

    state.encounteredFile(infoFile);
    state.successfullyPublishedFile(infoFile);

    // secrets is encountered - meaning it won't be deleted from the database! - but not published,
    // so the time is not updated.
    state.encounteredFile(secretsFile);

    // These two files should have the updated "published" time.
    assertEquals(state.getLastPublished(helloFile), state.getLastPublished(infoFile));
    // secrets should still have the old one.
    assertEquals(Instant.ofEpochMilli(3), state.getLastPublished(secretsFile));

    assertFalse(state.getNotEncounteredPaths().contains(secretsFile));
  }
}
