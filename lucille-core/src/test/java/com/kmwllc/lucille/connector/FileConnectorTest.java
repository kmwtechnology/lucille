package com.kmwllc.lucille.connector;

import static com.kmwllc.lucille.connector.FileConnector.FILE_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.kmwllc.lucille.connector.jdbc.DBTestHelper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.MockedStatic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kmwllc.lucille.connector.storageclient.LocalStorageClient;
import com.kmwllc.lucille.connector.storageclient.StorageClient;
import com.kmwllc.lucille.connector.storageclient.TraversalParams;
import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class FileConnectorTest {

  private static final Logger log = LoggerFactory.getLogger(FileConnectorTest.class);

  @Rule
  public final DBTestHelper dbHelper = new DBTestHelper("sm-db-test-start.sql");

  @Test
  public void testExecuteSuccessful() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/gcloudtraversal.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");

    try (MockedStatic<StorageClient> mockedStaticStorageClient = mockStatic(StorageClient.class)) {
      StorageClient mockCloudClient = mock(StorageClient.class);
      mockedStaticStorageClient.when(() -> StorageClient.createClients(any()))
          .thenReturn(Map.of(
              "file", new LocalStorageClient(),
              "gs", mockCloudClient));

      Connector connector = new FileConnector(config);
      connector.execute(publisher);

      verify(mockCloudClient, times(1)).init();
      verify(mockCloudClient, times(1)).traverse(any(Publisher.class), any(TraversalParams.class), eq(null));
      verify(mockCloudClient, times(1)).shutdown();
    }
  }

  @Test
  public void testExecuteSuccessfulExtraCloudConfig() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/multipleCloud.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");

    try (MockedStatic<StorageClient> mockCloudStorageClient = mockStatic(StorageClient.class)) {
      StorageClient mockForGoogle = mock(StorageClient.class);
      StorageClient mockForAzure = mock(StorageClient.class);
      mockCloudStorageClient.when(() -> StorageClient.createClients(any()))
          .thenReturn(Map.of(
              "file", new LocalStorageClient(),
              "gs", mockForGoogle,
              "https", mockForAzure));

      Connector connector = new FileConnector(config);
      connector.execute(publisher);
      verify(mockForGoogle, times(1)).init();
      verify(mockForGoogle, times(1)).traverse(any(Publisher.class), any(TraversalParams.class), eq(null));
      verify(mockForGoogle, times(1)).shutdown();

      // Azure will get started / shutdown, but not traversed
      verify(mockForAzure, times(1)).init();
      verify(mockForAzure, times(0)).traverse(any(Publisher.class), any(TraversalParams.class), eq(null));
      verify(mockForAzure, times(1)).shutdown();
    }
  }

  @Test
  public void testInitFailed() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/gcloudtraversal.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");

    try (MockedStatic<StorageClient> mockedStaticStorageClient = mockStatic(StorageClient.class)) {
      StorageClient mockCloudClient = mock(StorageClient.class);
      mockedStaticStorageClient.when(() -> StorageClient.createClients(any()))
          .thenReturn(Map.of(
              "file", new LocalStorageClient(),
              "gs", mockCloudClient));

      Connector connector = new FileConnector(config);

      // nothing takes place if a client fails to initialize
      doThrow(new IOException("Failed to initialize client")).when(mockCloudClient).init();
      assertThrows(ConnectorException.class, () -> connector.execute(publisher));
    }
  }

  @Test
  public void testPublishFilesFailed() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/gcloudtraversal.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");

    try (MockedStatic<StorageClient> mockCloudStorageClient = mockStatic(StorageClient.class)) {
      StorageClient mockCloudClient = mock(StorageClient.class);
      mockCloudStorageClient.when(() -> StorageClient.createClients(any()))
          .thenReturn(Map.of(
              "file", new LocalStorageClient(),
              "gs", mockCloudClient));

      Connector connector = new FileConnector(config);
      // the try catch block in FileConnector will catch any Exception class and throw a ConnectorException
      doThrow(new Exception("Failed to publish files")).when(mockCloudClient).traverse(any(Publisher.class), any(TraversalParams.class), eq(null));
      assertThrows(ConnectorException.class, () -> connector.execute(publisher));
      // verify that shutdown is called, even after a traversal fails
      verify(mockCloudClient, times(1)).shutdown();
    }
  }

  @Test
  public void testErrorDirectory() throws Exception {
    File tempDir = new File("temp");

    // copy faulty csv into temp directory
    File copy = new File("src/test/resources/FileConnectorTest/faulty.csv");
    org.apache.commons.io.FileUtils.copyFileToDirectory(copy, tempDir);

    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/faulty.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");
    Connector connector = new FileConnector(config);

    connector.execute(publisher);

    // verify error directory is made
    File errorDir = new File("error");
    File f = new File("error/faulty.csv");

    try {
      // verify error directory is made
      assertTrue(errorDir.exists());
      // verify file is moved inside error directory
      assertTrue(f.exists());
    } finally {
      // delete all created folders and files and reset the FileHandlerManager
      f.delete();
      errorDir.delete();
      tempDir.delete();
    }
  }

  @Test
  public void testSuccessfulDirectory() throws Exception {
    File tempDir = new File("temp");

    // copy successful csv into temp directory
    File copy = new File("src/test/resources/FileConnectorTest/defaults.csv");
    org.apache.commons.io.FileUtils.copyFileToDirectory(copy, tempDir);

    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/success.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");
    Connector connector = new FileConnector(config);

    connector.execute(publisher);

    // verify error directory is made
    File successDir = new File("success");
    File f = new File("success/defaults.csv");

    try {
      // verify error directory is made
      assertTrue(successDir.exists());
      // verify file is moved inside error directory
      assertTrue(f.exists());
    } finally {
      // delete all created folders and files and reset the FileHandlerManager
      f.delete();
      successDir.delete();
      tempDir.delete();
    }
  }

  @Test
  public void testExampleTraversal() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/example.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");
    Connector connector = new FileConnector(config);

    connector.execute(publisher);
    List<Document> documentList = messenger.getDocsSentForProcessing();

    // assert that all documents have been processed
    Assert.assertEquals(16, documentList.size());
    
    // find docs by ID in an order-independent way
    Document doc1 = documentList.stream().filter(d -> d.getId().equals("jsonHandled-a")).findAny().orElseThrow();
    assertEquals("Rustic Cows Mug", doc1.getString("name"));

    Document doc2 = documentList.stream().filter(d ->
        d.has(FILE_PATH) && d.getString(FILE_PATH).endsWith("helloWorld.txt.gz!helloWorld.txt")).findAny().orElseThrow();
    // we don't know second portion of the ID because it will be the MD5 hex of the absolute path of the zip entry which will be
    // different in different environments
    assertTrue(doc2.getId().startsWith("normal-"));

    Document doc3 = documentList.stream().filter(d ->
        d.has(FILE_PATH) && d.getString(FILE_PATH).endsWith("subDirWith2TxtFiles.zip!subDirWith2TxtFiles/first.txt")).findAny().orElseThrow();
    assertTrue(doc3.getId().startsWith("normal-"));

    assertEquals("First!", new String(doc3.getBytes("file_content")));

    Document doc4 = documentList.stream().filter(d ->
        d.has(FILE_PATH) && d.getString(FILE_PATH).endsWith("subDirWith2TxtFiles.zip!subDirWith2TxtFiles/second.txt")).findAny().orElseThrow();
    assertTrue(doc4.getId().startsWith("normal-"));
    assertEquals("Second!", new String(doc4.getBytes("file_content")));

    Document doc5 = documentList.stream().filter(d -> d.getId().equals("jsonHandled-b")).findAny().orElseThrow();
    assertEquals("Gorgeous Woman Mug", doc5.getString("name"));

    Document doc6 = documentList.stream().filter(d -> d.getId().equals("jsonHandled-c1")).findAny().orElseThrow();
    assertEquals("Awesome Night Mug", doc6.getString("name"));

    Document doc7 = documentList.stream().filter(d -> d.getId().equals("jsonHandled-c2")).findAny().orElseThrow();
    assertEquals("Ergonomic Mountains Mug", doc7.getString("name"));

    Document doc8 = documentList.stream().filter(d -> d.getId().equals("jsonHandled-c3")).findAny().orElseThrow();
    assertEquals("Refined Fog Mug", doc8.getString("name"));

    Document doc9 = documentList.stream().filter(d -> d.getId().equals("jsonHandled-c4")).findAny().orElseThrow();
    assertEquals("Sleek Castle Mug", doc9.getString("name"));

    Document doc10 = documentList.stream().filter(d -> d.getId().equals("jsonHandled-c5")).findAny().orElseThrow();
    assertEquals("Small City Mug", doc10.getString("name"));

    Document doc11 = documentList.stream().filter(d ->
        d.has(FILE_PATH) && d.getString(FILE_PATH).endsWith("subdir"+File.separatorChar+"e.yaml")).findAny().orElseThrow();
    assertTrue(doc11.getId().startsWith("normal-"));

    Document doc12 = documentList.stream().filter(d -> d.getId().equals("csvHandled-default.csv-1")).findAny().orElseThrow();
    assertTrue(doc12.getString("source").endsWith("subdirWith1csv1xml.tar.gz!subdirWith1csv1xml/default.csv"));

    Document doc13 = documentList.stream().filter(d -> d.getId().equals("csvHandled-default.csv-2")).findAny().orElseThrow();
    assertTrue(doc13.getString("source").endsWith("subdirWith1csv1xml.tar.gz!subdirWith1csv1xml/default.csv"));

    Document doc14 = documentList.stream().filter(d -> d.getId().equals("csvHandled-default.csv-3")).findAny().orElseThrow();
    assertTrue(doc14.getString("source").endsWith("subdirWith1csv1xml.tar.gz!subdirWith1csv1xml/default.csv"));

    Document doc15 = documentList.stream().filter(d -> d.getId().equals("xmlHandled-1001")).findAny().orElseThrow();
    assertEquals("<staff>\n" +
        "        <id>1001</id>\n" +
        "        <name>daniel</name>\n" +
        "        <role>software engineer</role>\n" +
        "        <salary currency=\"USD\">3000</salary>\n" +
        "        <bio>I am from San Diego</bio>\n" +
        "    </staff>", doc15.getString("xml"));

    Document doc16 = documentList.stream().filter(d -> d.getId().equals("xmlHandled-1002")).findAny().orElseThrow();
    assertEquals("<staff>\n" +
        "        <id>1002</id>\n" +
        "        <name>brian</name>\n" +
        "        <role>admin</role>\n" +
        "        <salary currency=\"EUR\">8000</salary>\n" +
        "        <bio>I enjoy reading</bio>\n" +
        "    </staff>", doc16.getString("xml"));
  }

  @Test
  public void testMultiplePathsSameClient() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/multiplePathsLocal.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");
    Connector connector = new FileConnector(config);

    connector.execute(publisher);
    List<Document> documentList = messenger.getDocsSentForProcessing();
    assertEquals(9, documentList.size());
  }

  @Test
  public void testMultiplePathsDifferentClients() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/multiplePathsLocalAndCloud.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");

    Connector connector;
    try (MockedStatic<StorageClient> mockStaticStorageClient = mockStatic(StorageClient.class)) {
      StorageClient s3StorageClient = mock(StorageClient.class);
      StorageClient googleStorageClient = mock(StorageClient.class);

      mockStaticStorageClient.when(() -> StorageClient.createClients(any()))
          .thenReturn(Map.of(
              "file", new LocalStorageClient(),
              "s3", s3StorageClient,
              "gs", googleStorageClient));

      doAnswer(invocationOnMock -> {
        publisher.publish(Document.create("a"));
        publisher.publish(Document.create("b"));
        publisher.publish(Document.create("c"));
        return null;
      }).when(s3StorageClient).traverse(any(), any(), any());

      doAnswer(invocationOnMock -> {
        publisher.publish(Document.create("d"));
        publisher.publish(Document.create("e"));
        publisher.publish(Document.create("f"));
        return null;
      }).when(googleStorageClient).traverse(any(), any(), any());

      connector = new FileConnector(config);
    }

    connector.execute(publisher);

    List<Document> documentList = messenger.getDocsSentForProcessing();
    assertEquals(9, documentList.size());
  }

  @Test
  public void testUnsupportedClientPath() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/multiplePathsSomeInvalid.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");
    Connector connector = new FileConnector(config);

    assertThrows(ConnectorException.class, () -> connector.execute(publisher));
  }

  @Test
  public void testPreventMultiplePathsAndMoveTo() throws Exception {
    Config config1 = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/multiplePathsMoveToProcessing.conf");
    assertThrows(IllegalArgumentException.class, () -> new FileConnector(config1));

    Config config2 = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/multiplePathsMoveToError.conf");
    assertThrows(IllegalArgumentException.class, () -> new FileConnector(config2));
  }

  @Test
  public void testIncrementalWithoutStateFailsFast() {
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/example.conf")
        .withValue("filterOptions.publishMode", ConfigValueFactory.fromAnyRef("incremental"));
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> new FileConnector(config));
    assertTrue(e.getMessage().contains("incremental"));
    assertTrue(e.getMessage().contains("state configuration"));
  }

  // There is a unit test for "traversalWithState" in FileConnectorStateManagerTest.java, which already had a database for testing.

  // Testing when the state configuration is empty.
  @Test
  @Execution(ExecutionMode.SAME_THREAD)
  public void testTraversalWithStateEmbedded() throws Exception {
    File stateDirectory = new File("state");
    File dbFile = new File("state/file-connector.mv.db");

    assertFalse(stateDirectory.exists());
    assertFalse(dbFile.exists());

    try {
      Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/emptyState.conf");
      TestMessenger messenger = new TestMessenger();
      Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");
      Connector connector = new FileConnector(config);

      // the stateManager doesn't get initialized until execution begins.
      connector.execute(publisher);

      // now the database file should exist.
      assertTrue(stateDirectory.isDirectory());
      assertTrue(dbFile.isFile());

      assertEquals(18, messenger.getDocsSentForProcessing().size());

      messenger = new TestMessenger();
      publisher = new PublisherImpl(config, messenger, "run", "pipeline1");

      connector.execute(publisher);

      // default full mode republishes all docs on subsequent runs
      assertEquals(18, messenger.getDocsSentForProcessing().size());
    } finally {
      try {
        Files.delete(dbFile.toPath());
        Files.delete(stateDirectory.toPath());
      } catch (IOException e) {
        fail("The state file / directory was not found - an exception may have been thrown during the test.");
      }
    }
  }

  @Test
  public void testLocalTraversal_DirWithSpacesAndSpecials_FromConf() throws Exception {
    Config cfg = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/specialsLocal.conf").resolve();

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(cfg, messenger, "run", "pipeline1");
    Connector connector = new FileConnector(cfg);

    connector.execute(publisher);
    List<Document> docs = messenger.getDocsSentForProcessing();

    assertEquals(3, docs.size());

    boolean sawText1 = docs.stream().anyMatch(d -> d.getString(FILE_PATH).endsWith("/directory%20with%20spaces/text%201.txt"));
    boolean sawWeird = docs.stream().anyMatch(d -> d.getString(FILE_PATH).endsWith("/directory%20with%20spaces/name(with)@special%20chars.txt"));
    boolean sawNormal = docs.stream().anyMatch(d -> d.getString(FILE_PATH).endsWith("/directory%20with%20spaces/normal.txt"));

    assertTrue(sawText1);
    assertTrue(sawWeird);
    assertTrue(sawNormal);
  }

  @Test(expected = IllegalArgumentException.class)
  // Constructing a FileConnector with sendTombstones=true and publishMode=full must throw.
  public void testSendTombstonesRequiresIncrementalMode() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/state.conf")
        .withValue("filterOptions.publishMode", ConfigValueFactory.fromAnyRef("full"))
        .withValue("filterOptions.sendTombstones", ConfigValueFactory.fromAnyRef("true"));
    new FileConnector(config);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSendTombstonesRequiresPublishMode() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/state.conf")
        .withValue("filterOptions.sendTombstones", ConfigValueFactory.fromAnyRef("true"));
    new FileConnector(config);
  }

  @Test
  public void testSendTombstonesFalseDoesNotRequireIncrementalMode() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/state.conf")
        .withValue("filterOptions.publishMode", ConfigValueFactory.fromAnyRef("full"))
        .withValue("filterOptions.sendTombstones", ConfigValueFactory.fromAnyRef("false"));
    new FileConnector(config);
  }

  // Verifies that a second incremental run with no file changes produces zero docs and zero tombstones.
  @Test
  public void testTraversalWithStateAndMultiplePathsIncremental() throws Exception {
    // For each archive that passes the encountered step but fails includeFile, markAllEntriesEncountered is called so their DB
    // entries are flagged as seen. Without this, every archive entry would appear expired and generate a tombstone despite the
    // files still existing.
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/stateMultiplePaths.conf")
        .withValue("filterOptions.publishMode", ConfigValueFactory.fromAnyRef("incremental"))
        .withValue("filterOptions.sendTombstones", ConfigValueFactory.fromAnyRef("true"));

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");

    Connector connector = new FileConnector(config);
    connector.execute(publisher);
    assertEquals(21, messenger.getDocsSentForProcessing().size());

    messenger = new TestMessenger();
    publisher = new PublisherImpl(config, messenger, "run", "pipeline1");

    // Second run: nothing changed, so nothing is republished and no tombstones are generated.
    connector.execute(publisher);
    List<Document> secondRunDocs = messenger.getDocsSentForProcessing();
    long tombstoneCount = secondRunDocs.stream()
        .filter(doc -> Boolean.TRUE.equals(doc.getBoolean(FileConnector.EXPIRED)))
        .count();
    assertEquals(0, secondRunDocs.size());
    assertEquals(0, tombstoneCount);
  }

  // Verifies that a traversal with a state db and using multiple paths behaves as expected with full mode.
  @Test
  public void testTraversalWithStateAndMultiplePaths() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/stateMultiplePaths.conf");

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");

    Connector connector = new FileConnector(config);
    connector.execute(publisher);
    assertEquals(21, messenger.getDocsSentForProcessing().size());

    // second run in default full mode republishes everything.
    messenger = new TestMessenger();
    publisher = new PublisherImpl(config, messenger, "run", "pipeline1");

    connector.execute(publisher);
    assertEquals(21, messenger.getDocsSentForProcessing().size());
  }

  // Verifies that full mode does not emit tombstones even when a previously tracked path disappears between runs.
  @Test
  public void testTraversalWithPathRemovedFullModeDoesNotPublishTombstones() throws Exception {
    // Run 1 uses stateMultiplePaths.conf (example/ + defaults.csv = 21 docs). Run 2 uses state.conf
    // (example/ only). defaults.csv is now encountered=false in the DB, but since neither config
    // sets sendTombstones, no tombstone is generated. Run 2 publishes 18 docs (example/ in full mode).
    Config configWithTwoPaths = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/stateMultiplePaths.conf");
    Config configSinglePath = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/state.conf");

    TestMessenger messenger1 = new TestMessenger();
    Publisher publisher1 = new PublisherImpl(configWithTwoPaths, messenger1, "run", "pipeline1");
    Connector connector1 = new FileConnector(configWithTwoPaths);
    connector1.execute(publisher1);
    assertEquals(21, messenger1.getDocsSentForProcessing().size());

    TestMessenger messenger2 = new TestMessenger();
    Publisher publisher2 = new PublisherImpl(configSinglePath, messenger2, "run", "pipeline1");
    Connector connector2 = new FileConnector(configSinglePath);
    connector2.execute(publisher2);

    List<Document> secondRunDocs = messenger2.getDocsSentForProcessing();
    long tombstoneCount = secondRunDocs.stream()
        .filter(doc -> Boolean.TRUE.equals(doc.getBoolean(FileConnector.EXPIRED)))
        .count();
    assertEquals(18, secondRunDocs.size());
    assertEquals(0, tombstoneCount);
  }

  // Verifies that when sendTombstones is false, deleting a file between runs does not produce tombstones.
  @Test
  public void testTraversalWithIncrementalTombstonesOff() throws Exception {
    // Creates a temp directory with three files, deletes one, and confirms
    // zero docs on the second run (no tombstone for the deleted file, no republish for unchanged files).
    File tempDir = new File("temp-tombstones-off");
    assertFalse(tempDir.exists());
    tempDir.mkdir();

    File file1 = new File(tempDir, "file1.txt");
    File file2 = new File(tempDir, "file2.txt");
    File file3 = new File(tempDir, "file3.txt");

    Files.writeString(file1.toPath(), "Content 1");
    Files.writeString(file2.toPath(), "Content 2");
    Files.writeString(file3.toPath(), "Content 3");

    try {
      Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/stateMultiplePaths.conf")
          .withValue("paths", ConfigValueFactory.fromIterable(List.of(tempDir.toURI().toString())))
          .withValue("filterOptions.publishMode", ConfigValueFactory.fromAnyRef("incremental"))
          .withValue("filterOptions.sendTombstones", ConfigValueFactory.fromAnyRef("false"));

      TestMessenger messenger1 = new TestMessenger();
      Publisher publisher1 = new PublisherImpl(config, messenger1, "run1", "pipeline1");
      Connector connector = new FileConnector(config);

      // First run: all 3 files published
      connector.execute(publisher1);
      assertEquals(3, messenger1.getDocsSentForProcessing().size());

      // Delete file2 between runs
      Files.delete(file2.toPath());

      // Second run: file2 was deleted, but sendTombstones is false so no tombstone should be published
      // file1 and file3 are unchanged, so they are not republished either
      TestMessenger messenger2 = new TestMessenger();
      Publisher publisher2 = new PublisherImpl(config, messenger2, "run2", "pipeline1");
      Connector connector2 = new FileConnector(config);
      connector2.execute(publisher2);

      List<Document> secondRunDocs = messenger2.getDocsSentForProcessing();
      assertEquals(0, secondRunDocs.size());
    } finally {
      FileUtils.deleteDirectory(tempDir);
    }
  }

  // Verifies that files generate tombstones and modified files are re-published in the same incremental run (when configured). Uses
  // an empty state to trigger a database being created on disk to replicate a production environment
  @Test
  public void testTombstonesWithFileSystemChanges() throws Exception {

    File tempDir = new File("temp-tombstone");
    assertFalse(tempDir.exists());
    tempDir.mkdir();

    File file1 = new File(tempDir, "file1.txt");
    File file2 = new File(tempDir, "file2.txt");
    File file3 = new File(tempDir, "file3.txt");

    Files.writeString(file1.toPath(), "Content 1");
    Files.writeString(file2.toPath(), "Content 2");
    Files.writeString(file3.toPath(), "Content 3");

    try {
      // Config with state and incremental mode with tombstones enabled
      Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/emptyState.conf")
          .withValue("paths", ConfigValueFactory.fromIterable(List.of(tempDir.toURI().toString())))
          .withValue("filterOptions.publishMode", ConfigValueFactory.fromAnyRef("incremental"))
          .withValue("filterOptions.sendTombstones", ConfigValueFactory.fromAnyRef("true"));

      TestMessenger messenger1 = new TestMessenger();
      Publisher publisher1 = new PublisherImpl(config, messenger1, "run1", "pipeline1");
      Connector connector = new FileConnector(config);

      // First run, publish all 3 files
      connector.execute(publisher1);
      assertEquals(3, messenger1.getDocsSentForProcessing().size());

      // Verify no tombstones on first run
      long firstRunTombstones = messenger1.getDocsSentForProcessing().stream()
          .filter(doc -> Boolean.TRUE.equals(doc.getBoolean(FileConnector.EXPIRED)))
          .count();
      assertEquals(0, firstRunTombstones);

      // Delete file2, modify file1, leave file3 alone.
      // Set file1's modification time explicitly to a point after the first run's traversalInstant just in case
      Files.delete(file2.toPath());
      Files.writeString(file1.toPath(), "Modified Content 1");
      Files.setLastModifiedTime(file1.toPath(), java.nio.file.attribute.FileTime.from(Instant.now().plusSeconds(5)));

      // Second run, should publish modified file1 + tombstone for deleted file2
      Connector connector2 = new FileConnector(config);
      TestMessenger messenger2 = new TestMessenger();
      Publisher publisher2 = new PublisherImpl(config, messenger2, "run2", "pipeline1");
      connector2.execute(publisher2);

      List<Document> secondRunDocs = messenger2.getDocsSentForProcessing();
      assertEquals(2, secondRunDocs.size());  // 1 modified file + 1 tombstone

      // Verify 1 tombstone
      long tombstoneCount = secondRunDocs.stream()
          .filter(doc -> Boolean.TRUE.equals(doc.getBoolean(FileConnector.EXPIRED)))
          .count();
      assertEquals(1, tombstoneCount);

      // Verify tombstone is for file2
      Document tombstone = secondRunDocs.stream()
          .filter(doc -> Boolean.TRUE.equals(doc.getBoolean(FileConnector.EXPIRED)))
          .findFirst()
          .orElseThrow();
      assertTrue(tombstone.getString(FileConnector.FILE_PATH).contains("file2.txt"));
      assertTrue(tombstone.isSkipped());

      // Verify non-tombstone is for modified file1
      Document modifiedDoc = secondRunDocs.stream()
          .filter(doc -> !Boolean.TRUE.equals(doc.getBoolean(FileConnector.EXPIRED)))
          .findFirst()
          .orElseThrow();
      assertTrue(modifiedDoc.getString(FileConnector.FILE_PATH).contains("file1.txt"));

    } finally {
      FileUtils.deleteDirectory(tempDir);

      // Needed since our db is on disk
      FileUtils.deleteDirectory(new File("state"));
    }
  }

}
