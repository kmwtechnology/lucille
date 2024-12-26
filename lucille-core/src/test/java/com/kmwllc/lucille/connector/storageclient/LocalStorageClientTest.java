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
    LocalStorageClient localStorageClient = new LocalStorageClient(
        new URI("src/test/resources/StorageClientTest/testPublishFilesDefault/"), "file_",
        List.of(Pattern.compile(".*\\.DS_Store$")), List.of(),
        Map.of(), ConfigFactory.parseMap(
        Map.of(
            "json", Map.of()
        )
    ));

    localStorageClient.init();
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    localStorageClient.traverse(publisher);

    List<Document> documents = messenger.getDocsSentForProcessing();

    Assert.assertEquals(12, documents.size());

    Document doc1 = documents.get(0);
    Assert.assertEquals("a", doc1.getId());
    Assert.assertEquals("Rustic Cows Mug", doc1.getString("name"));

    Document doc2 = documents.get(1);
    Assert.assertEquals("d", doc2.getId());
    Assert.assertEquals("Incredible People Mug", doc2.getString("name"));

    Document doc3 = documents.get(2);
    Assert.assertEquals("e", doc3.getId());
    Assert.assertEquals("Awesome Wood Mug", doc3.getString("name"));

    Document doc4 = documents.get(3);
    Assert.assertEquals("f1", doc4.getId());
    Assert.assertEquals("Awesome Night Mug", doc4.getString("name"));

    Document doc5 = documents.get(4);
    Assert.assertEquals("f2", doc5.getId());
    Assert.assertEquals("Ergonomic Mountains Mug", doc5.getString("name"));

    Document doc6 = documents.get(5);
    Assert.assertEquals("f3", doc6.getId());
    Assert.assertEquals("Refined Fog Mug", doc6.getString("name"));

    Document doc7 = documents.get(6);
    Assert.assertEquals("f4", doc7.getId());
    Assert.assertEquals("Sleek Castle Mug", doc7.getString("name"));

    Document doc8 = documents.get(7);
    Assert.assertEquals("f5", doc8.getId());
    Assert.assertEquals("Small City Mug", doc8.getString("name"));

    Document doc9 = documents.get(8);
    Assert.assertEquals("file_76ad6cc964453df6031d9660ba4f29d6", doc9.getId());
    Assert.assertTrue(doc9.getString(FILE_PATH).endsWith("subdir1/e.yaml"));

    Document doc10 = documents.get(9);
    Assert.assertEquals("file_660eb217f0ca5cfbfa51d8150fb2ede0", doc10.getId());
    Assert.assertTrue(doc10.getString(FILE_PATH).endsWith("subdir1/e.json.gz"));

    Document doc11 = documents.get(10);
    Assert.assertEquals("c", doc11.getId());
    Assert.assertEquals("Awesome City Mug", doc11.getString("name"));

    Document doc12 = documents.get(11);
    Assert.assertEquals("b", doc12.getId());
    Assert.assertEquals("Gorgeous Woman Mug", doc12.getString("name"));

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

    Assert.assertEquals(19, docs.size());

    // check documents published from jsonlCsvAndFolderWithFooTxt.tar
    Document doc1 = docs.get(0);
    Assert.assertTrue(doc1.getId().endsWith("default.csv-1"));
    Assert.assertEquals("default.csv", doc1.getString("filename"));
    Document doc2 = docs.get(1);
    Assert.assertTrue(doc2.getId().endsWith("default.csv-2"));
    Assert.assertEquals("default.csv", doc2.getString("filename"));
    Document doc3 = docs.get(2);
    Assert.assertTrue(doc3.getId().endsWith("default.csv-3"));
    Assert.assertEquals("default.csv", doc3.getString("filename"));
    Document doc4 = docs.get(3);
    Assert.assertEquals("2", doc4.getId());
    Assert.assertEquals("Gorgeous Woman Mug", doc4.getString("name"));
    Document doc5 = docs.get(4);
    Assert.assertEquals("3", doc5.getId());
    Assert.assertEquals("Awesome City Mug", doc5.getString("name"));
    Document doc6 = docs.get(5);
    Assert.assertTrue(doc6.getString("file_path").endsWith("FolderWithFooTxt/foo.txt"));

    // check documents published from textFiles.tar
    Document doc7 = docs.get(6);
    Assert.assertTrue(doc7.getString("file_path").endsWith("helloWorld.txt"));
    Document doc8 = docs.get(7);
    Assert.assertTrue(doc8.getString("file_path").endsWith("goodbye.txt"));

    // check documents published from jsonlCsvAndFolderWithFooTxt.tar.gz
    Document doc9 = docs.get(8);
    Assert.assertTrue(doc9.getId().endsWith("default.csv-1"));
    Assert.assertEquals("default.csv", doc9.getString("filename"));
    Document doc10 = docs.get(9);
    Assert.assertTrue(doc10.getId().endsWith("default.csv-2"));
    Assert.assertEquals("default.csv", doc10.getString("filename"));
    Document doc11 = docs.get(10);
    Assert.assertTrue(doc11.getId().endsWith("default.csv-3"));
    Assert.assertEquals("default.csv", doc11.getString("filename"));
    Document doc12 = docs.get(11);
    Assert.assertEquals("2", doc12.getId());
    Assert.assertEquals("Gorgeous Woman Mug", doc12.getString("name"));
    Document doc13 = docs.get(12);
    Assert.assertEquals("3", doc13.getId());
    Assert.assertEquals("Awesome City Mug", doc13.getString("name"));
    Document doc14 = docs.get(13);
    Assert.assertTrue(doc14.getString("file_path").endsWith("FolderWithFooTxt/foo.txt"));
    // check documents published from zippedFolder.zip
    Document doc15 = docs.get(14);
    Assert.assertTrue(doc15.getString("file_path").endsWith("zippedFolder/foo.txt"));
    // check document published from hello.zip
    Document doc16 = docs.get(15);
    Assert.assertTrue(doc16.getString("file_path").endsWith("hello"));
    // check documents published from zipped.csv
    Document doc17 = docs.get(16);
    Assert.assertTrue(doc17.getId().endsWith("zipped.csv-1"));
    Assert.assertEquals("zipped.csv", doc17.getString("filename"));
    Document doc18 = docs.get(17);
    Assert.assertTrue(doc18.getId().endsWith("zipped.csv-2"));
    Assert.assertEquals("zipped.csv", doc18.getString("filename"));
    Document doc19 = docs.get(18);
    Assert.assertTrue(doc19.getId().endsWith("zipped.csv-3"));
    Assert.assertEquals("zipped.csv", doc19.getString("filename"));

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
