package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.FILE_PATH;
import static com.kmwllc.lucille.connector.FileConnector.GET_FILE_CONTENT;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.connector.VFSConnector;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.core.fileHandler.CSVFileHandler;
import com.kmwllc.lucille.core.fileHandler.JsonFileHandler;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.Assert;
import org.junit.Test;

public class LocalStorageClientTest {

  @Test
  public void testInitFileHandlers() throws Exception{
    LocalStorageClient localStorageClient = new LocalStorageClient(new URI("src/test/resources/StorageClientTest"), "",
        List.of(), List.of(), Map.of(), ConfigFactory.parseMap(
            Map.of("json", Map.of(), "csv", Map.of())
    ));

    localStorageClient.init();
    // check that the file handlers are initialized, 3 in this case as json and jsonl keys are populated with same fileHandler
    assertEquals(3, localStorageClient.fileHandlers.size());
    assertInstanceOf(JsonFileHandler.class, localStorageClient.fileHandlers.get("json"));
    assertInstanceOf(JsonFileHandler.class, localStorageClient.fileHandlers.get("jsonl"));
    assertEquals(localStorageClient.fileHandlers.get("json"), localStorageClient.fileHandlers.get("jsonl"));
    assertInstanceOf(CSVFileHandler.class, localStorageClient.fileHandlers.get("csv"));
    localStorageClient.shutdown();
  }

  @Test
  public void testShutdown() throws Exception{
    LocalStorageClient localStorageClient = new LocalStorageClient(new URI("src/test/resources/StorageClientTest"), "",
        List.of(), List.of(), Map.of(), ConfigFactory.parseMap(
        Map.of("json", Map.of(), "csv", Map.of())
    ));

    localStorageClient.init();
    // check that the file handlers are initialized, 3 in this case as json and jsonl keys are populated with same fileHandler
    assertEquals(3, localStorageClient.fileHandlers.size());
    localStorageClient.shutdown();

    // check that the file handlers are cleared
    assertEquals(0, localStorageClient.fileHandlers.size());
  }

  @Test
  public void testPublishValidFiles() throws Exception {
    LocalStorageClient localStorageClient = new LocalStorageClient(new URI("src/test/resources/StorageClientTest/testPublishFilesDefault"), "file_",
        List.of(), List.of(), Map.of(), ConfigFactory.empty());
    localStorageClient.init();
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");
    localStorageClient.traverse(publisher);

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

    localStorageClient.shutdown();
  }

  @Test
  public void testPublishFilesWithExclude() throws Exception {
    LocalStorageClient localStorageClient = new LocalStorageClient(new URI("src/test/resources/StorageClientTest/testPublishFilesDefault"), "file_",
        List.of(Pattern.compile(".*/subdir1/.*$")), List.of(Pattern.compile(".*/[a-c]\\.json$")), Map.of(), ConfigFactory.empty());
    localStorageClient.init();
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    localStorageClient.traverse(publisher);
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
    LocalStorageClient localStorageClient = new LocalStorageClient(
        new URI("src/test/resources/StorageClientTest/testPublishFilesDefault/a.json"), "file_",
        List.of(), List.of(),
        Map.of(), ConfigFactory.parseMap(
            Map.of(GET_FILE_CONTENT, false)
    ));

    localStorageClient.init();
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    localStorageClient.traverse(publisher);

    Assert.assertEquals(1, messenger.getDocsSentForProcessing().size());
    Document doc = messenger.getDocsSentForProcessing().get(0);
    assertFalse(doc.has(FileConnector.CONTENT));
  }

  @Test
  public void testPublishUsingFileHandler() throws Exception {

  }


  @Test
  public void testPublishOnCompressedAndArchived() throws Exception {
    LocalStorageClient localStorageClient = new LocalStorageClient(
        new URI("src/test/resources/StorageClientTest/testCompressedAndArchived"), "",
        List.of(), List.of(),
        Map.of(), ConfigFactory.parseMap(
        Map.of(
            "json", Map.of(),
            "csv", Map.of(),
            "handleArchivedFiles", true,
            "handleCompressedFiles", true
        )));

    localStorageClient.init();
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");
    localStorageClient.traverse(publisher);

    // remove .DS_Store file if present
    List<Document> docs = messenger.getDocsSentForProcessing().stream()
        .filter(doc -> !doc.has(FILE_PATH) || !doc.getString(FILE_PATH).endsWith(".DS_Store"))
        .toList();

    for (Document doc : docs) {
      System.out.println(doc.asMap());
    }

    Assert.assertEquals(19, docs.size());

    // check documents published from jsonlCsvAndFolderWithFooTxt.tar
    Document doc1 = docs.get(0);
    Assert.assertEquals("default.csv-1", doc1.getId());
    Assert.assertEquals("default.csv", doc1.getString("source"));
    Document doc2 = docs.get(1);
    Assert.assertEquals("default.csv-2", doc2.getId());
    Assert.assertEquals("default.csv", doc2.getString("source"));
    Document doc3 = docs.get(2);
    Assert.assertEquals("default.csv-3", doc3.getId());
    Assert.assertEquals("default.csv", doc3.getString("source"));
    Document doc4 = docs.get(3);
    Assert.assertEquals("2", doc4.getId());
    Assert.assertEquals("Gorgeous Woman Mug", doc4.getString("name"));
    Document doc5 = docs.get(4);
    Assert.assertEquals("3", doc5.getId());
    Assert.assertEquals("Awesome City Mug", doc5.getString("name"));
    Document doc6 = docs.get(5);
    Assert.assertEquals("FolderWithFooTxt/foo.txt", doc6.getString("file_path"));

    // check documents published from textFiles.tar
    Document doc7 = docs.get(6);
    Assert.assertEquals("helloWorld.txt", doc7.getString("file_path"));
    Document doc8 = docs.get(7);
    Assert.assertEquals("goodbye.txt", doc8.getString("file_path"));

    // check documents published from jsonlCsvAndFolderWithFooTxt.tar.gz
    Document doc9 = docs.get(8);
    Assert.assertEquals("default.csv-1", doc9.getId());
    Assert.assertEquals("default.csv", doc9.getString("source"));
    Document doc10 = docs.get(9);
    Assert.assertEquals("default.csv-2", doc10.getId());
    Assert.assertEquals("default.csv", doc10.getString("source"));
    Document doc11 = docs.get(10);
    Assert.assertEquals("default.csv-3", doc11.getId());
    Assert.assertEquals("default.csv", doc11.getString("source"));
    Document doc12 = docs.get(11);
    Assert.assertEquals("2", doc12.getId());
    Assert.assertEquals("Gorgeous Woman Mug", doc12.getString("name"));
    Document doc13 = docs.get(12);
    Assert.assertEquals("3", doc13.getId());
    Assert.assertEquals("Awesome City Mug", doc13.getString("name"));
    Document doc14 = docs.get(13);
    Assert.assertEquals("FolderWithFooTxt/foo.txt", doc14.getString("file_path"));
    // check documents published from zippedFolder.zip
    Document doc15 = docs.get(14);
    Assert.assertEquals("zippedFolder/foo.txt", doc15.getString("file_path"));
    // check document published from hello.zip
    Document doc16 = docs.get(15);
    Assert.assertEquals("hello", doc16.getString("file_path"));
    // check documents published from zipped.csv
    Document doc17 = docs.get(16);
    Assert.assertEquals("zipped.csv-1", doc17.getId());
    Assert.assertEquals("zipped.csv", doc17.getString("source"));
    Document doc18 = docs.get(17);
    Assert.assertEquals("zipped.csv-2", doc18.getId());
    Assert.assertEquals("zipped.csv", doc18.getString("source"));
    Document doc19 = docs.get(18);
    Assert.assertEquals("zipped.csv-3", doc19.getId());
    Assert.assertEquals("zipped.csv", doc19.getString("source"));

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
    LocalStorageClient localStorageClient = new LocalStorageClient(
        new URI("temp/defaults.csv"), "",
        List.of(), List.of(),
        Map.of(), ConfigFactory.parseMap(
        Map.of(
            "moveToAfterProcessing", "success"
        )));

    localStorageClient.init();

    localStorageClient.traverse(publisher);

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
    LocalStorageClient localStorageClient = new LocalStorageClient(
        new URI("temp/faulty.csv"), "",
        List.of(), List.of(),
        Map.of(), ConfigFactory.parseMap(
        Map.of(
            "csv", Map.of(),
            "moveToErrorFolder", "error"
        )));

    localStorageClient.init();
    localStorageClient.traverse(publisher);

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
}
