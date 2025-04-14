package com.kmwllc.lucille.connector.storageclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.kmwllc.lucille.connector.jdbc.DBTestHelper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.net.URI;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;

public class StorageClientStateManagerTest {

  private static final String helloFile = "/hello.txt";
  private static final String infoFile = "/files/info.txt";
  private static final String secretsFile = "/files/subdir/secrets.txt";

  private static final String filesDirectory = "/files/";
  private static final String subdirDirectory = "/files/subdir/";

  private static final List<String> allFilePaths = List.of(helloFile, infoFile, secretsFile);
  private static final List<String> allDirectoryPaths = List.of("/", filesDirectory, subdirDirectory);

  private static final List<String> allFileAndDirectoryPaths = List.of(helloFile, infoFile, secretsFile, "/", filesDirectory, subdirDirectory);

  @Rule
  public final DBTestHelper dbHelper = new DBTestHelper("org.h2.Driver", "jdbc:h2:mem:test", "", "",
      "sm-db-test-start.sql", "sm-db-test-end.sql");

  @Test
  public void testStateManagerRootDirectory() throws Exception {
    assertEquals(1, dbHelper.checkNumConnections());
    Config config = ConfigFactory.parseResourcesAnySyntax("StorageClientStateManagerTest/config.conf");
    StorageClientStateManager manager = new StorageClientStateManager(config);
    manager.init();

    StorageClientState state = manager.getStateForTraversal(URI.create("/"), "file");

    // making sure the times are read correctly from the start file...
    assertNotNull(state.getLastPublished(helloFile));
    assertNotNull(state.getLastPublished(infoFile));
    assertNotNull(state.getLastPublished(secretsFile));

    for (String filePath : allFileAndDirectoryPaths) {
      state.markFileOrDirectoryEncountered(filePath);
    }

    for (String filePath : allFilePaths) {
      state.successfullyPublishedFile(filePath);
    }

    assertEquals(0, state.getNewDirectoryPaths().size());
    assertEquals(0, state.getPathsToDelete().size());

    // TODO: add to test with a "write" method, once it is completed...

    manager.shutdown();
    assertEquals(1, dbHelper.checkNumConnections());
  }

  @Test
  public void testStateManagerSubdirDirectory() throws Exception {
    assertEquals(1, dbHelper.checkNumConnections());
    Config config = ConfigFactory.parseResourcesAnySyntax("StorageClientStateManagerTest/config.conf");
    StorageClientStateManager manager = new StorageClientStateManager(config);
    manager.init();

    StorageClientState state = manager.getStateForTraversal(URI.create("/files/subdir/"), "file");

    // should only have state entry for the relevant file...
    assertNull(state.getLastPublished(helloFile));
    assertNull(state.getLastPublished(infoFile));
    assertNotNull(state.getLastPublished(secretsFile));

    state.markFileOrDirectoryEncountered("/files/subdir/");
    state.markFileOrDirectoryEncountered("/files/subdir/secrets.txt");
    state.successfullyPublishedFile("/files/subdir/secrets.txt");

    // TODO: add to test with a "write" method, once it is completed...

    manager.shutdown();
    assertEquals(1, dbHelper.checkNumConnections());
  }

  @Test
  public void testStateManagerSingleFile() throws Exception {
    assertEquals(1, dbHelper.checkNumConnections());
    Config config = ConfigFactory.parseResourcesAnySyntax("StorageClientStateManagerTest/config.conf");
    StorageClientStateManager manager = new StorageClientStateManager(config);
    manager.init();

    StorageClientState state = manager.getStateForTraversal(URI.create("/files/info.txt"), "file");

    // should only have state entry for the relevant file...
    assertNull(state.getLastPublished(helloFile));
    assertNotNull(state.getLastPublished(infoFile));
    assertNull(state.getLastPublished(secretsFile));

    state.markFileOrDirectoryEncountered("/files/");
    state.markFileOrDirectoryEncountered("/files/info.txt");
    state.successfullyPublishedFile("/files/info.txt");

    // TODO: add to test with a "write" method, once it is completed...

    manager.shutdown();
    assertEquals(1, dbHelper.checkNumConnections());
  }

  @Test
  public void testStateManagerOnNewDirectory() throws Exception {
    assertEquals(1, dbHelper.checkNumConnections());
    Config config = ConfigFactory.parseResourcesAnySyntax("StorageClientStateManagerTest/config.conf");
    StorageClientStateManager manager = new StorageClientStateManager(config);
    manager.init();

    StorageClientState state = manager.getStateForTraversal(URI.create("/newdir/"), "file");

    // shouldn't have gotten state for any of the files
    assertNull(state.getLastPublished(helloFile));
    assertNull(state.getLastPublished(infoFile));
    assertNull(state.getLastPublished(secretsFile));

    state.markFileOrDirectoryEncountered("/newdir/");
    state.markFileOrDirectoryEncountered("/newdir/new.txt");
    state.successfullyPublishedFile("/newdir/new.txt");

    assertEquals(1, state.getNewDirectoryPaths().size());
    assertEquals(1, state.getNewlyPublishedFilePaths().size());

    // TODO: Add to test with a "write" method

    manager.shutdown();
    assertEquals(1, dbHelper.checkNumConnections());
  }
}
