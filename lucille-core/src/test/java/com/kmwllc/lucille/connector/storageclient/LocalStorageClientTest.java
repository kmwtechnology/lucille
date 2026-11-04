package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.FILE_PATH;
import static com.kmwllc.lucille.connector.FileConnector.GET_FILE_CONTENT;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.attribute.PosixFilePermissions;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class LocalStorageClientTest {

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  // per-class directory names, so test classes running in parallel forks never share one
  private static final String TEMP_DIR = "localStorageClientTest-temp";
  private static final String ERROR_DIR = "localStorageClientTest-error";
  private static final String SUCCESS_DIR = "localStorageClientTest-success";

  @Test
  public void testPublishValidFiles() throws Exception {
    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    LocalStorageClient localStorageClient = new LocalStorageClient();
    TraversalParams params = new TraversalParams(ConfigFactory.empty(), URI.create("src/test/resources/StorageClientTest/testPublishFilesDefault"), "file_");
    localStorageClient.start();
    localStorageClient.traverse(publisher, params);

    String[] fileNames = {"a.json", "b.json", "c.json", "d.json",
        "subdir1/e.json", "subdir1/e.json.gz", "subdir1/e.yaml", "subdir1/f.jsonl"};
    int docCount = 0;
    for (Document doc : messenger.getDocsSentForProcessing()) {
      String docId = doc.getId();
      String filePath = doc.getString(FileConnector.FILE_PATH);
      // skip if it's an automatically generated Finder file because the directory was opened
      if (filePath.endsWith(".DS_Store")) continue;
      String content = new String(doc.getBytes(FileConnector.CONTENT));
      Assert.assertTrue(docId.startsWith("file_"));
      Assert.assertTrue(Arrays.stream(fileNames).anyMatch(filePath::endsWith));
      if (filePath.endsWith("c.json")) {
        Assert.assertTrue(content.contains("\"artist\":\"Lou Levit\""));
      }
      if (filePath.endsWith("subdir1/e.yaml")) {
        Assert.assertTrue(content.contains("slug: Awesome-Wood-Mug"));
      }
      docCount++;
    }
    Assert.assertEquals(8, docCount);

    localStorageClient.stop();
  }

  // A StorageClient can be traversed with state using any FileStateTracker, without a FileConnector or a database.
  @Test
  public void testTraverseWithInMemoryState() throws Exception {
    URI root = URI.create("src/test/resources/StorageClientTest/testPublishFilesDefault");
    Config config = ConfigFactory.parseMap(Map.of("filterOptions.publishMode", "incremental"));
    TraversalParams params = new TraversalParams(config, root, "file_");
    InMemoryFileStateTracker state = new InMemoryFileStateTracker();

    LocalStorageClient localStorageClient = new LocalStorageClient();
    localStorageClient.start();

    try {
      TestMessenger firstRun = new TestMessenger();
      localStorageClient.traverse(new PublisherImpl(ConfigFactory.empty(), firstRun, "run1", "pipeline1"), params, state);
      long published = firstRun.getDocsSentForProcessing().stream()
          .filter(doc -> !doc.getString(FileConnector.FILE_PATH).endsWith(".DS_Store")).count();
      assertEquals(8, published);
      assertEquals(firstRun.getDocsSentForProcessing().size(), state.getLastPublishedMap().size());

      // nothing was modified since it was published, so the second run publishes nothing, but still encounters every file
      state.startNewRun();
      TestMessenger secondRun = new TestMessenger();
      localStorageClient.traverse(new PublisherImpl(ConfigFactory.empty(), secondRun, "run2", "pipeline1"), params, state);
      assertEquals(0, secondRun.getDocsSentForProcessing().size());
      assertTrue(state.getEncountered().containsAll(state.getLastPublishedMap().keySet()));
    } finally {
      localStorageClient.stop();
    }
  }

  @Test
  public void testPublishFilesWithExclude() throws Exception {
    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Config connectorConfig = ConfigFactory.parseMap(Map.of(
        "filterOptions", Map.of(
            "includes", List.of(".*[a-c]\\.json$"),
            "excludes", List.of(".*subdir1.*$")
        )
    ));

    LocalStorageClient localStorageClient = new LocalStorageClient();
    TraversalParams params = new TraversalParams(connectorConfig, URI.create("src/test/resources/StorageClientTest/testPublishFilesDefault"), "file_");
    localStorageClient.start();

    localStorageClient.traverse(publisher, params);

    Assert.assertEquals(3, messenger.getDocsSentForProcessing().size());
    String[] fileNames = {"a.json", "b.json", "c.json"};
    for (Document doc : messenger.getDocsSentForProcessing()) {
      String docId = doc.getId();
      String filePath = doc.getString(FileConnector.FILE_PATH);
      String content = new String(doc.getBytes(FileConnector.CONTENT));
      Assert.assertTrue(Arrays.stream(fileNames).anyMatch(filePath::endsWith));
      if (filePath.endsWith("a.json")) {
        Assert.assertTrue(content.contains("\"filename\":\"400_106547e2f83b.jpg\""));
      }
      if (filePath.endsWith("b.json")) {
        Assert.assertTrue(content.contains("\"imageHash\":\"1aaeac2de7c48e4e7773b1f92138291f\""));
      }
      if (filePath.endsWith("c.json")) {
        Assert.assertTrue(content.contains("\"productImg\":\"mug-400_6812876c6c27.jpg\""));
      }
    }

    localStorageClient.stop();
  }

  @Test
  public void testSkipFileContent() throws Exception {
    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    LocalStorageClient localStorageClient = new LocalStorageClient();

    Config connectorConfig = ConfigFactory.parseMap(Map.of(
        "fileOptions", Map.of(GET_FILE_CONTENT, false)
    ));

    TraversalParams params = new TraversalParams(connectorConfig, URI.create("src/test/resources/StorageClientTest/testPublishFilesDefault/a.json"), "file_");

    localStorageClient.start();

    localStorageClient.traverse(publisher, params);

    Assert.assertEquals(1, messenger.getDocsSentForProcessing().size());
    Document doc = messenger.getDocsSentForProcessing().get(0);
    assertFalse(doc.has(FileConnector.CONTENT));
  }

  @Test
  public void testPublishUsingFileHandler() throws Exception {
    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Config connectorConfig = ConfigFactory.parseMap(Map.of(
        "fileHandlers", Map.of("json", Map.of()),
        "filterOptions", Map.of("excludes", List.of(".*\\.DS_Store$"))
    ));

    LocalStorageClient localStorageClient = new LocalStorageClient();
    TraversalParams params = new TraversalParams(connectorConfig, URI.create("src/test/resources/StorageClientTest/testPublishFilesDefault/"), "file_");

    localStorageClient.start();
    localStorageClient.traverse(publisher, params);
    List<Document> docs = messenger.getDocsSentForProcessing();

    assertEquals(12, docs.size());

    Document doc1 = docs.stream().filter(d -> d.getId().equals("a")).findAny().orElseThrow();
    assertEquals("Rustic Cows Mug", doc1.getString("name"));

    Document doc2 = docs.stream().filter(d -> d.getId().equals("b")).findAny().orElseThrow();
    assertEquals("Gorgeous Woman Mug", doc2.getString("name"));

    Document doc3 = docs.stream().filter(d -> d.getId().equals("c")).findAny().orElseThrow();
    assertEquals("Awesome City Mug", doc3.getString("name"));

    Document doc4 = docs.stream().filter(d -> d.getId().equals("d")).findAny().orElseThrow();
    assertEquals("Incredible People Mug", doc4.getString("name"));

    Document doc5 = docs.stream().filter(d -> d.getId().equals("e")).findAny().orElseThrow();
    assertEquals("Awesome Wood Mug", doc5.getString("name"));

    Document doc6 = docs.stream().filter(d -> d.has(FILE_PATH) &&
        d.getString(FILE_PATH).endsWith("subdir1/e.json.gz")).findAny().orElseThrow();

    Document doc7 = docs.stream().filter(d ->
        d.has(FILE_PATH) && d.getString(FILE_PATH).endsWith("subdir1/e.yaml")).findAny().orElseThrow();

    Document doc8 = docs.stream().filter(d -> d.getId().equals("f1")).findAny().orElseThrow();
    assertEquals("Awesome Night Mug", doc8.getString("name"));

    Document doc9 = docs.stream().filter(d -> d.getId().equals("f2")).findAny().orElseThrow();
    assertEquals("Ergonomic Mountains Mug", doc9.getString("name"));

    Document doc10 = docs.stream().filter(d -> d.getId().equals("f3")).findAny().orElseThrow();
    assertEquals("Refined Fog Mug", doc10.getString("name"));

    Document doc11 = docs.stream().filter(d -> d.getId().equals("f4")).findAny().orElseThrow();
    assertEquals("Sleek Castle Mug", doc11.getString("name"));

    Document doc12 = docs.stream().filter(d -> d.getId().equals("f5")).findAny().orElseThrow();
    assertEquals("Small City Mug", doc12.getString("name"));

    localStorageClient.stop();
  }


  @Test
  public void testPublishOnCompressedAndArchived() throws Exception {

    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Config connectorConfig = ConfigFactory.parseMap(Map.of(
        "fileHandlers", Map.of(
            "json", Map.of(),
            "csv", Map.of()
        ),
        "fileOptions", Map.of(
            "handleArchivedFiles", true,
            "handleCompressedFiles", true
        ),
        "filterOptions", Map.of("excludes", List.of(".*\\.DS_Store$"))
    ));

    LocalStorageClient localStorageClient = new LocalStorageClient();
    TraversalParams params = new TraversalParams(connectorConfig, URI.create("src/test/resources/StorageClientTest/testCompressedAndArchived"), "");

    localStorageClient.start();
    localStorageClient.traverse(publisher, params);

    List<Document> docs = messenger.getDocsSentForProcessing();

    assertEquals(19, docs.size());

    docs.stream().filter(d -> d.has(FILE_PATH) && d.getString(FILE_PATH).endsWith("hello.zip!hello")).findAny().orElseThrow();

    Document doc = docs.stream().filter(d -> d.getId().equals("default.csv-1") && d.has("source") &&
        d.getString("source").endsWith("jsonlCsvAndFolderWithFooTxt.tar!default.csv")).findAny().orElseThrow();
    assertEquals("a", doc.getString("field1"));
    assertEquals("b", doc.getString("field2"));
    assertEquals("c", doc.getString("field3"));

    doc = docs.stream().filter(d -> d.getId().equals("default.csv-2") && d.has("source") &&
        d.getString("source").endsWith("jsonlCsvAndFolderWithFooTxt.tar!default.csv")).findAny().orElseThrow();
    assertEquals("d", doc.getString("field1"));
    assertEquals("e,f", doc.getString("field2"));
    assertEquals("g", doc.getString("field3"));

    doc = docs.stream().filter(d -> d.getId().equals("default.csv-3") && d.has("source") &&
        d.getString("source").endsWith("jsonlCsvAndFolderWithFooTxt.tar!default.csv")).findAny().orElseThrow();
    assertEquals("x", doc.getString("field1"));
    assertEquals("y", doc.getString("field2"));
    assertEquals("z", doc.getString("field3"));

    List<Document> docsWithId2 = docs.stream().filter(d -> d.getId().equals("2")).toList();
    assertEquals(2, docsWithId2.size());
    assertEquals("Gorgeous Woman Mug", docsWithId2.get(0).getString("name"));
    assertEquals(docsWithId2.get(0), docsWithId2.get(1));

    List<Document> docsWithId3 = docs.stream().filter(d -> d.getId().equals("3")).toList();
    assertEquals(2, docsWithId3.size());
    assertEquals("Awesome City Mug", docsWithId3.get(0).getString("name"));
    assertEquals(docsWithId3.get(0), docsWithId3.get(1));

    docs.stream().filter(d ->
        d.has(FILE_PATH) && d.getString(FILE_PATH).endsWith("jsonlCsvAndFolderWithFooTxt.tar!FolderWithFooTxt/foo.txt")).findAny().orElseThrow();

    doc = docs.stream().filter(d -> d.getId().equals("default.csv-1") && d.has("source") &&
        d.getString("source").endsWith("jsonlCsvAndFolderWithFooTxt.tar.gz!default.csv")).findAny().orElseThrow();
    assertEquals("a", doc.getString("field1"));
    assertEquals("b", doc.getString("field2"));
    assertEquals("c", doc.getString("field3"));

    doc = docs.stream().filter(d -> d.getId().equals("default.csv-2") && d.has("source") &&
        d.getString("source").endsWith("jsonlCsvAndFolderWithFooTxt.tar.gz!default.csv")).findAny().orElseThrow();
    assertEquals("d", doc.getString("field1"));
    assertEquals("e,f", doc.getString("field2"));
    assertEquals("g", doc.getString("field3"));

    doc = docs.stream().filter(d -> d.getId().equals("default.csv-3") && d.has("source") &&
        d.getString("source").endsWith("jsonlCsvAndFolderWithFooTxt.tar.gz!default.csv")).findAny().orElseThrow();
    assertEquals("x", doc.getString("field1"));
    assertEquals("y", doc.getString("field2"));
    assertEquals("z", doc.getString("field3"));

    docs.stream().filter(d ->
        d.has(FILE_PATH) && d.getString(FILE_PATH).endsWith("jsonlCsvAndFolderWithFooTxt.tar.gz!FolderWithFooTxt/foo.txt")).findAny().orElseThrow();

    docs.stream().filter(d ->
        d.has(FILE_PATH) && d.getString(FILE_PATH).endsWith("textFiles.tar!helloWorld.txt")).findAny().orElseThrow();

    docs.stream().filter(d ->
        d.has(FILE_PATH) && d.getString(FILE_PATH).endsWith("textFiles.tar!goodbye.txt")).findAny().orElseThrow();

    docs.stream().filter(d -> d.getId().equals("zipped.csv-1")).findAny().orElseThrow();

    docs.stream().filter(d -> d.getId().equals("zipped.csv-2")).findAny().orElseThrow();

    docs.stream().filter(d -> d.getId().equals("zipped.csv-3")).findAny().orElseThrow();

    docs.stream().filter(d ->
        d.has(FILE_PATH) && d.getString(FILE_PATH).endsWith("zippedFolder.zip!zippedFolder/foo.txt")).findAny().orElseThrow();

    localStorageClient.stop();
  }

  @Test
  public void testMoveProcessedFiles() throws Exception{
    File tempDir = new File(TEMP_DIR);

    // copy successful csv into temp directory
    File copy = new File("src/test/resources/FileConnectorTest/defaults.csv");
    org.apache.commons.io.FileUtils.copyFileToDirectory(copy, tempDir);

    Config connectorConfig = ConfigFactory.parseMap(Map.of(
        "fileOptions", Map.of("moveToAfterProcessing", SUCCESS_DIR)
    ));

    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");
    LocalStorageClient localStorageClient = new LocalStorageClient();
    TraversalParams params = new TraversalParams(connectorConfig, URI.create(TEMP_DIR + "/defaults.csv"), "");

    localStorageClient.start();

    localStorageClient.traverse(publisher, params);

    // verify error directory is made
    File successDir = new File(SUCCESS_DIR);
    File f = new File(SUCCESS_DIR + "/defaults.csv");

    try {
      // verify error directory is made
      assertTrue(successDir.exists());
      // verify file is moved inside error directory
      assertTrue(f.exists());
    } finally {
      // delete everything we've created, shutdown the storage client
      f.delete();
      successDir.delete();
      tempDir.delete();
      localStorageClient.stop();
    }
  }

  // As we use URIs for the processed/error paths, want to make sure a relative and an absolute
  // path in the Config will work for moveToAfter error / processing.
  @Test
  public void testMoveProcessedFilesAbsolutePath() throws Exception {
    File tempDir = new File(TEMP_DIR);

    // copy successful csv into temp directory
    File copy = new File("src/test/resources/FileConnectorTest/defaults.csv");
    org.apache.commons.io.FileUtils.copyFileToDirectory(copy, tempDir);

    Config connectorConfig = ConfigFactory.parseMap(Map.of(
        "fileOptions", Map.of("moveToAfterProcessing", Paths.get(SUCCESS_DIR).toAbsolutePath().toString())
    ));

    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");
    LocalStorageClient localStorageClient = new LocalStorageClient();
    TraversalParams params = new TraversalParams(connectorConfig, URI.create(TEMP_DIR + "/defaults.csv"), "");

    localStorageClient.start();

    localStorageClient.traverse(publisher, params);

    // verify error directory is made
    File successDir = new File(SUCCESS_DIR);
    File f = new File(SUCCESS_DIR + "/defaults.csv");

    try {
      // verify error directory is made
      assertTrue(successDir.exists());
      // verify file is moved inside error directory
      assertTrue(f.exists());
    } finally {
      // delete everything we've created, shutdown the storage client
      f.delete();
      successDir.delete();
      tempDir.delete();
      localStorageClient.stop();
    }
  }

  @Test
  public void testMoveErrorFiles() throws Exception{
    File tempDir = new File(TEMP_DIR);

    // copy faulty csv into temp directory
    File copy = new File("src/test/resources/FileConnectorTest/faulty.csv");
    org.apache.commons.io.FileUtils.copyFileToDirectory(copy, tempDir);

    Config connectorConfig = ConfigFactory.parseMap(Map.of(
        "fileOptions", Map.of("moveToErrorFolder", ERROR_DIR),
        "fileHandlers", Map.of("csv", Map.of())
    ));

    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");
    // localStorageClient that handles csv files
    LocalStorageClient localStorageClient = new LocalStorageClient();
    TraversalParams params = new TraversalParams(connectorConfig, URI.create(TEMP_DIR + "/faulty.csv"), "");

    localStorageClient.start();
    localStorageClient.traverse(publisher, params);

    // verify error directory is made
    File errorDir = new File(ERROR_DIR);
    File f = new File(ERROR_DIR + "/faulty.csv");

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
      localStorageClient.stop();
    }
  }

  @Test
  public void testGetFileContentStream() throws Exception {
    File testFile = new File("src/test/resources/StorageClientTest/hello.txt");

    LocalStorageClient client = new LocalStorageClient();
    client.start();
    InputStream stream = client.getFileContentStream(testFile.toURI());

    assertEquals("Hello there.", new String(stream.readAllBytes()));
  }

  // Ensuring that providing an absolute path to the LocalStorageClient works as appropriate - doesn't remove the first
  // "/" as TraversalParams does, instead, falls back on its specific implementation of getStartingDirectory()
  @Test
  public void testAbsolutePath() throws Exception {
    Path defaultAbsolutePath = Paths.get("src/test/resources/StorageClientTest/testPublishFilesDefault").toAbsolutePath();

    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    LocalStorageClient localStorageClient = new LocalStorageClient();
    TraversalParams params = new TraversalParams(ConfigFactory.empty(), TraversalParams.parsePathOrURI(defaultAbsolutePath.toString()), "file_");
    localStorageClient.start();
    localStorageClient.traverse(publisher, params);

    assertEquals(8, publisher.numPublished());
  }

  @Test
  public void testCutoff() throws Exception {
    Config connectorConfig = ConfigFactory.parseMap(Map.of(
        // cutoff behavior should match under incremental mode.
        // only including files modified in the last 10 hours
        "filterOptions", Map.of("lastModifiedCutoff", "10h", "publishMode", "incremental")
    ));

    File oldFile = new File("src/test/resources/StorageClientTest/modifiedDateFiles/old.txt");
    File alsoOldFile = new File("src/test/resources/StorageClientTest/modifiedDateFiles/subDir/alsoOld.txt");
    File newFile = new File("src/test/resources/StorageClientTest/modifiedDateFiles/new.txt");

    // Want to make sure the test will work - set the three files to be modified VERY recently or VERY long ago.
    assertTrue(oldFile.setLastModified(Instant.ofEpochMilli(1).toEpochMilli()));
    assertTrue(alsoOldFile.setLastModified(Instant.ofEpochMilli(1).toEpochMilli()));
    assertTrue(newFile.setLastModified(Instant.now().toEpochMilli()));

    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    LocalStorageClient localStorageClient = new LocalStorageClient();
    localStorageClient.start();

    TraversalParams params = new TraversalParams(connectorConfig, URI.create("src/test/resources/StorageClientTest/modifiedDateFiles"), "");
    localStorageClient.traverse(publisher, params);
    // only "new file" should be published - others are BEFORE the cutoff.
    assertEquals(1, publisher.numPublished());
  }

  @Test
  public void testPathsToSkip() throws Exception {
    String subdir1Uri = URI.create("src/test/resources/StorageClientTest/testPublishFilesDefault/subdir1/").toString();

    if (!subdir1Uri.endsWith("/")) {
      subdir1Uri += "/";
    }

    Config connectorConfig = ConfigFactory.parseMap(Map.of(
        "filterOptions", Map.of(
            "pathsToSkip", List.of(subdir1Uri),
            "excludes", List.of(".*\\.DS_Store$")
        )
    ));

    pathsToSkipTesting(connectorConfig);
  }

  @Test
  public void testPathsToSkipNoTrailingSlash() throws Exception {
    String subdir1Uri = URI.create("src/test/resources/StorageClientTest/testPublishFilesDefault/subdir1").toString();

    if (subdir1Uri.endsWith("/")) {
      subdir1Uri = subdir1Uri.substring(0, subdir1Uri.length() - 1);
    }

    Config connectorConfig = ConfigFactory.parseMap(Map.of(
        "filterOptions", Map.of(
            "pathsToSkip", List.of(subdir1Uri.toString()),
            "excludes", List.of(".*\\.DS_Store$")
        )
    ));

    pathsToSkipTesting(connectorConfig);
  }

  @Test
  public void testPathsToSkipRelative() throws Exception {
    String subdir1Path = Paths.get("src/test/resources/StorageClientTest/testPublishFilesDefault/subdir1").toString();

    if (!subdir1Path.endsWith("/")) {
      subdir1Path += "/";
    }

    Config connectorConfig = ConfigFactory.parseMap(Map.of(
        "filterOptions", Map.of(
            "pathsToSkip", List.of(subdir1Path),
            "excludes", List.of(".*\\.DS_Store$")
        )
    ));

    pathsToSkipTesting(connectorConfig);
  }

  @Test
  public void testPathsToSkipRelativeNoTrailingSlash() throws Exception {
    String subdir1Path = Paths.get("src/test/resources/StorageClientTest/testPublishFilesDefault/subdir1").toString();

    if (subdir1Path.endsWith("/")) {
      subdir1Path = subdir1Path.substring(0, subdir1Path.length() - 1);
    }

    Config connectorConfig = ConfigFactory.parseMap(Map.of(
        "filterOptions", Map.of(
            "pathsToSkip", List.of(subdir1Path),
            "excludes", List.of(".*\\.DS_Store$")
        )
    ));

    pathsToSkipTesting(connectorConfig);
  }

  @Test
  public void testPathsToSkipAbsolute() throws Exception {
    String subdir1Path = Paths.get("src/test/resources/StorageClientTest/testPublishFilesDefault/subdir1").toAbsolutePath().toString();

    if (!subdir1Path.endsWith("/")) {
      subdir1Path += "/";
    }

    Config connectorConfig = ConfigFactory.parseMap(Map.of(
        "filterOptions", Map.of(
            "pathsToSkip", List.of(subdir1Path),
            "excludes", List.of(".*\\.DS_Store$")
        )
    ));

    pathsToSkipTesting(connectorConfig);
  }

  @Test
  public void testPathsToSkipAbsoluteNoTrailingSlash() throws Exception {
    String subdir1Path = Paths.get("src/test/resources/StorageClientTest/testPublishFilesDefault/subdir1").toAbsolutePath().toString();

    if (subdir1Path.endsWith("/")) {
      subdir1Path = subdir1Path.substring(0, subdir1Path.length() - 1);
    }

    Config connectorConfig = ConfigFactory.parseMap(Map.of(
        "filterOptions", Map.of(
            "pathsToSkip", List.of(subdir1Path),
            "excludes", List.of(".*\\.DS_Store$")
        )
    ));

    pathsToSkipTesting(connectorConfig);
  }

  // pathsToSkip entries must be absolute URIs. A file:// URI for subdir1 skips it entirely,
  // leaving only the 4 root-level json files.
  private void pathsToSkipTesting(Config connectorConfig) throws Exception {
    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    LocalStorageClient localStorageClient = new LocalStorageClient();
    TraversalParams params = new TraversalParams(connectorConfig,
        URI.create("src/test/resources/StorageClientTest/testPublishFilesDefault"), "");
    localStorageClient.start();
    localStorageClient.traverse(publisher, params);

    List<Document> docs = messenger.getDocsSentForProcessing();
    assertEquals(4, docs.size());
    assertTrue(docs.stream().noneMatch(d -> d.getString(FileConnector.FILE_PATH).contains("subdir1")));

    localStorageClient.stop();
  }

  // A missing root (e.g. an unmounted share) must fail the traversal, not finish it having seen no files.
  @Test
  public void testTraverseMissingRootFails() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");
    URI missing = new File(tempFolder.getRoot(), "not-mounted").toURI();

    LocalStorageClient client = new LocalStorageClient();
    client.start();
    try {
      assertThrows(NoSuchFileException.class,
          () -> client.traverse(publisher, new TraversalParams(ConfigFactory.empty(), missing, ""), new InMemoryFileStateTracker()));
      assertEquals(0, messenger.getDocsSentForProcessing().size());
    } finally {
      client.stop();
    }
  }

  @Test
  public void testTraverseUnreadableRootFails() throws Exception {
    assumePosixAndNotRoot();
    Path root = tempFolder.newFolder("unreadable-root").toPath();
    Files.writeString(root.resolve("a.txt"), "a");
    Files.setPosixFilePermissions(root, PosixFilePermissions.fromString("---------"));

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");
    LocalStorageClient client = new LocalStorageClient();
    client.start();
    try {
      assertThrows(AccessDeniedException.class,
          () -> client.traverse(publisher, new TraversalParams(ConfigFactory.empty(), root.toUri(), "")));
    } finally {
      Files.setPosixFilePermissions(root, PosixFilePermissions.fromString("rwx------"));
      client.stop();
    }
  }

  // A directory under the root that can't be read is skipped, but the files under it stay encountered in state, so they
  // are not tombstoned. The rest of the traversal continues.
  @Test
  public void testUnreadableSubdirectoryIsKeptInState() throws Exception {
    assumePosixAndNotRoot();
    Path root = tempFolder.newFolder("partly-readable").toPath();
    Path subdir = Files.createDirectory(root.resolve("sub"));
    Files.writeString(root.resolve("a.txt"), "a");
    Files.writeString(subdir.resolve("b.txt"), "b");
    // a sibling sharing the directory's name as a prefix must not be protected by it
    Files.writeString(root.resolve("sub.txt"), "c");

    String aURI = root.resolve("a.txt").toUri().toString();
    String bURI = subdir.resolve("b.txt").toUri().toString();
    String siblingURI = root.resolve("sub.txt").toUri().toString();

    InMemoryFileStateTracker state = new InMemoryFileStateTracker();
    LocalStorageClient client = new LocalStorageClient();
    client.start();

    try {
      TestMessenger messenger = new TestMessenger();
      client.traverse(new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1"),
          new TraversalParams(ConfigFactory.empty(), root.toUri(), ""), state);
      assertEquals(3, messenger.getDocsSentForProcessing().size());

      state.startNewRun();
      Files.delete(root.resolve("sub.txt"));
      Files.setPosixFilePermissions(subdir, PosixFilePermissions.fromString("---------"));

      messenger = new TestMessenger();
      client.traverse(new PublisherImpl(ConfigFactory.empty(), messenger, "run2", "pipeline1"),
          new TraversalParams(ConfigFactory.empty(), root.toUri(), ""), state);

      assertEquals(1, messenger.getDocsSentForProcessing().size());
      assertTrue(state.getEncountered().contains(aURI));
      assertTrue(state.getEncountered().contains(bURI));
      assertFalse(state.getEncountered().contains(siblingURI));
      // the directory itself is not a file, so it gets no entry of its own
      assertFalse(state.getEncountered().contains(subdir.toUri().toString()));
    } finally {
      Files.setPosixFilePermissions(subdir, PosixFilePermissions.fromString("rwx------"));
      client.stop();
    }
  }

  // A listing that ends early leaves files unseen that would look deleted, so it fails the traversal.
  @Test
  public void testDirectoryListingFailurePartwayFails() throws Exception {
    Path root = tempFolder.newFolder("listing").toPath();
    LocalStorageClient client = new LocalStorageClient();
    LocalStorageClient.LocalFileVisitor visitor = client.new LocalFileVisitor(null,
        new TraversalParams(ConfigFactory.empty(), root.toUri(), ""), null);

    java.io.IOException listingFailure = new java.io.IOException("mount dropped");
    assertEquals(listingFailure, assertThrows(java.io.IOException.class, () -> visitor.postVisitDirectory(root, listingFailure)));
    assertEquals(java.nio.file.FileVisitResult.CONTINUE, visitor.postVisitDirectory(root, null));
  }

  // A move folder whose parents don't exist yet is created, rather than failing the move after the file was published.
  @Test
  public void testMoveFileCreatesMissingParents() throws Exception {
    Path source = tempFolder.newFile("a.txt").toPath();
    Path folder = tempFolder.getRoot().toPath().resolve("processed/2024/11");

    LocalStorageClient client = new LocalStorageClient();
    client.moveFile(source.toUri(), folder.toUri());

    assertFalse(Files.exists(source));
    assertTrue(Files.exists(folder.resolve("a.txt")));
  }

  // Permission tests need POSIX permissions, and root can read anything regardless of them.
  private static void assumePosixAndNotRoot() {
    Assume.assumeTrue(java.nio.file.FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
    Assume.assumeFalse("root".equals(System.getProperty("user.name")));
  }
}
