package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.FILE_PATH;
import static com.kmwllc.lucille.connector.FileConnector.GET_FILE_CONTENT;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.connector.VFSConnector;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.Assert;
import org.junit.Test;

public class LocalStorageClientTest {

  @Test
  public void testPublishValidFiles() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    LocalStorageClient localStorageClient = new LocalStorageClient();
    TraversalParams params = new TraversalParams(new URI("src/test/resources/StorageClientTest/testPublishFilesDefault"), "file_", List.of(), List.of(), ConfigFactory.empty());
    localStorageClient.init();
    localStorageClient.traverse(publisher, params);

    String[] fileNames = {"a.json", "b.json", "c.json", "d.json",
        "subdir1"+File.separatorChar+"e.json", "subdir1"+File.separatorChar+"e.json.gz", "subdir1"+File.separatorChar+"e.yaml", "subdir1"+File.separatorChar+"f.jsonl"};
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

    localStorageClient.shutdown();
  }

  @Test
  public void testPublishFilesWithExclude() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    LocalStorageClient localStorageClient = new LocalStorageClient();
    TraversalParams params = new TraversalParams(new URI("src/test/resources/StorageClientTest/testPublishFilesDefault"), "file_", List.of(Pattern.compile(".*[a-c]\\.json$")), List.of(Pattern.compile(".*subdir1.*$")), ConfigFactory.empty());
    localStorageClient.init();

    localStorageClient.traverse(publisher, params);

    for (Document d : messenger.getDocsSentForProcessing()) {
      System.out.println(d);
    }

    Assert.assertEquals(3, messenger.getDocsSentForProcessing().size());
    String[] fileNames = {"a.json", "b.json", "c.json"};
    for (Document doc : messenger.getDocsSentForProcessing()) {
      String docId = doc.getId();
      String filePath = doc.getString(VFSConnector.FILE_PATH);
      String content = new String(doc.getBytes(VFSConnector.CONTENT));
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

    localStorageClient.shutdown();
  }

  @Test
  public void testSkipFileContent() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    LocalStorageClient localStorageClient = new LocalStorageClient();

    TraversalParams params = new TraversalParams(new URI("src/test/resources/StorageClientTest/testPublishFilesDefault/a.json"),
        "file_",
        List.of(), List.of(), ConfigFactory.parseMap(
        Map.of(GET_FILE_CONTENT, false)
    ));

    localStorageClient.init();

    localStorageClient.traverse(publisher, params);

    Assert.assertEquals(1, messenger.getDocsSentForProcessing().size());
    Document doc = messenger.getDocsSentForProcessing().get(0);
    assertFalse(doc.has(FileConnector.CONTENT));
  }

  @Test
  public void testPublishUsingFileHandler() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    LocalStorageClient localStorageClient = new LocalStorageClient();
    TraversalParams params = new TraversalParams(new URI("src/test/resources/StorageClientTest/testPublishFilesDefault/"), "file_",
        List.of(), List.of(Pattern.compile(".*\\.DS_Store$")), ConfigFactory.parseMap(
        Map.of(
            "json", Map.of()
        )));

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
        d.getString(FILE_PATH).endsWith("subdir1"+File.separatorChar+"e.json.gz")).findAny().orElseThrow();

    Document doc7 = docs.stream().filter(d ->
        d.has(FILE_PATH) && d.getString(FILE_PATH).endsWith("subdir1"+File.separatorChar+"e.yaml")).findAny().orElseThrow();

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

    localStorageClient.shutdown();
  }


  @Test
  public void testPublishOnCompressedAndArchived() throws Exception {

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    LocalStorageClient localStorageClient = new LocalStorageClient();
    TraversalParams params = new TraversalParams(new URI("src/test/resources/StorageClientTest/testCompressedAndArchived"), "",
        List.of(), List.of(Pattern.compile(".*\\.DS_Store$")), ConfigFactory.parseMap(
        Map.of(
            "json", Map.of(),
            "csv", Map.of(),
            "handleArchivedFiles", true,
            "handleCompressedFiles", true
        )));

    localStorageClient.init();
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

    localStorageClient.shutdown();
  }

  @Test
  public void testMoveProcessedFiles() throws Exception{
    File tempDir = new File("temp");

    // copy successful csv into temp directory
    File copy = new File("src/test/resources/FileConnectorTest/defaults.csv");
    org.apache.commons.io.FileUtils.copyFileToDirectory(copy, tempDir);

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");
    LocalStorageClient localStorageClient = new LocalStorageClient();
    TraversalParams params = new TraversalParams(new URI("temp/defaults.csv"), "",
        List.of(), List.of(), ConfigFactory.parseMap(
        Map.of(
            "moveToAfterProcessing", "success"
        )));

    localStorageClient.init();

    localStorageClient.traverse(publisher, params);

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
      localStorageClient.shutdown();
    }
  }

  @Test
  public void testMoveErrorFiles() throws Exception{
    File tempDir = new File("temp");

    // copy faulty csv into temp directory
    File copy = new File("src/test/resources/FileConnectorTest/faulty.csv");
    org.apache.commons.io.FileUtils.copyFileToDirectory(copy, tempDir);

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");
    // localStorageClient that handles csv files
    LocalStorageClient localStorageClient = new LocalStorageClient();
    TraversalParams params = new TraversalParams(new URI("temp/faulty.csv"), "",
        List.of(), List.of(), ConfigFactory.parseMap(
        Map.of(
            "csv", Map.of(),
            "moveToErrorFolder", "error"
        )));

    localStorageClient.init();
    localStorageClient.traverse(publisher, params);

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
      localStorageClient.shutdown();
    }
  }

  @Test
  public void testGetFileContentStream() throws Exception {
    File testFile = new File("src/test/resources/StorageClientTest/hello.txt");

    LocalStorageClient client = new LocalStorageClient();
    InputStream stream = client.getFileContentStream(testFile.toURI());

    assertEquals("Hello there.", new String(stream.readAllBytes()));
  }
}
