package com.kmwllc.lucille.connector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.connector.storageclients.StorageClient;
import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.FileHandlerManager;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.MockedStatic;

public class FileConnectorTest {

  FileSystem mockFileSystem;

  @Before
  public void setUp() throws Exception {
    mockFileSystem = mock(FileSystem.class);
    when(mockFileSystem.getPath(any())).thenReturn(mock(Path.class));
  }

  @Test
  public void testExecuteForCloudFiles() throws Exception {
    Config config = ConfigFactory.load("FileConnectorTest/gcloudtraversal.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");
    Connector connector = new FileConnector(config);

    try (MockedStatic<StorageClient> mockCloudStorageClient = mockStatic(StorageClient.class)) {
      StorageClient storageClient = mock(StorageClient.class);
      mockCloudStorageClient.when(() -> StorageClient.getClient(any(), any(), any(), any(), any(), any(), any()))
          .thenReturn(storageClient);

      connector.execute(publisher);
      verify(storageClient, times(1)).init();
      verify(storageClient, times(1)).publishFiles();
    }
    connector.close();
  }

  @Test
  public void testExecuteWithLocalFiles() throws Exception {
    Config config = ConfigFactory.load("FileConnectorTest/localfs.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");
    Connector connector = new FileConnector(config);

    connector.execute(publisher);
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
    connector.close();
  }

  @Test
  public void testExecuteWithLocalFilesFiltered() throws Exception {
    Config config = ConfigFactory.load("FileConnectorTest/localfs_filtered.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");
    Connector connector = new FileConnector(config);

    connector.execute(publisher);
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
    connector.close();
  }

  @Test
  public void testHandleJsonFiles() throws Exception {
    Config config = ConfigFactory.load("FileConnectorTest/handleJson.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");
    Connector connector = new FileConnector(config);

    connector.execute(publisher);

    List<Document> documentList = messenger.getDocsSentForProcessing();
    // assert that all documents have been processed
    Assert.assertEquals(5, documentList.size());

    Document jsonDoc2 = documentList.get(0);
    Document jsonDoc3 = documentList.get(1);
    Document jsonDoc1 = documentList.get(2);
    Document jsonDoc4 = documentList.get(4);
    Document normalDoc = documentList.get(3);

    assertEquals("prefix1", jsonDoc1.getId());
    assertEquals("prefix2", jsonDoc2.getId());
    assertEquals("prefix3", jsonDoc3.getId());
    assertEquals("prefix4", jsonDoc4.getId());
    assertEquals("12", normalDoc.getString("file_size_bytes"));
    assertEquals("Hello World!", new String(normalDoc.getBytes("file_content")));
    connector.close();
  }

  @Test
  public void testHandleCSVFiles() throws Exception {
    Config config = ConfigFactory.load("FileConnectorTest/handleCSV.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");
    Connector connector = new FileConnector(config);

    connector.execute(publisher);

    List<Document> documentList = messenger.getDocsSentForProcessing();

    assertEquals(5, documentList.size());

    Document csvDoc1 = documentList.get(0);
    Document csvDoc2 = documentList.get(1);
    Document csvDoc3 = documentList.get(2);
    Document csvDoc4 = documentList.get(3);
    Document csvDoc6 = documentList.get(4);
    assertEquals("example.csv-1", csvDoc1.getId());
    assertEquals("example.csv-2", csvDoc2.getId());
    assertEquals("example.csv-3", csvDoc3.getId());
    assertEquals("example.csv-4", csvDoc4.getId());
    assertEquals("example.csv-6", csvDoc6.getId());

    assertEquals("foo", csvDoc1.getString("field1"));
    assertEquals("bar", csvDoc1.getString("field2"));
    assertEquals("baz", csvDoc1.getString("field3"));
    assertEquals("1", csvDoc1.getString("csvLineNumber"));

    assertEquals("giraffe", csvDoc2.getString("field1"));
    assertEquals("apple", csvDoc2.getString("field2"));
    assertEquals("pineapple", csvDoc2.getString("field3"));
    assertEquals("2", csvDoc2.getString("csvLineNumber"));

    assertEquals("quartz", csvDoc3.getString("field1"));
    assertEquals("zinc", csvDoc3.getString("field2"));
    assertEquals("copper", csvDoc3.getString("field3"));
    assertEquals("3", csvDoc3.getString("csvLineNumber"));

    assertEquals("val1", csvDoc4.getString("field1"));
    assertEquals("val2", csvDoc4.getString("field2"));
    assertEquals("val3", csvDoc4.getString("field3"));
    assertEquals("4", csvDoc4.getString("csvLineNumber"));

    assertEquals("abc", csvDoc6.getString("field1"));
    assertEquals("def", csvDoc6.getString("field2"));
    assertEquals("ghi", csvDoc6.getString("field3"));
    // skipped line 5 as it was empty
    assertEquals("6", csvDoc6.getString("csvLineNumber"));

    FileHandlerManager.close();
  }

  @Test
  public void testErrorDirectory() throws Exception {
    File tempDir = new File("temp");

    // copy faulty csv into temp directory
    File copy = new File("src/test/resources/FileConnectorTest/faulty.csv");
    org.apache.commons.io.FileUtils.copyFileToDirectory(copy, tempDir);

    Config config = ConfigFactory.load("FileConnectorTest/faulty.conf");
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
      FileHandlerManager.close();
    }
  }

  @Test
  public void testSuccessfulDirectory() throws Exception {
    File tempDir = new File("temp");

    // copy successful csv into temp directory
    File copy = new File("src/test/resources/FileConnectorTest/defaults.csv");
    org.apache.commons.io.FileUtils.copyFileToDirectory(copy, tempDir);

    Config config = ConfigFactory.load("FileConnectorTest/success.conf");
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
      FileHandlerManager.close();
    }
  }



  // we ignore the tests related to compression/achived files ATM, need more investigation on resource management
  @Ignore
  @Test
  public void testHandleZippedJsonFiles() throws Exception {
    Config config = ConfigFactory.load("FileConnectorTest/testHandleZip.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");
    Connector connector = new FileConnector(config);

    connector.execute(publisher);
    List<Document> documentList = messenger.getDocsSentForProcessing();
    // assert that all documents have been processed
    Assert.assertEquals(5, documentList.size());

    Document jsonDoc2 = documentList.get(0);
    Document jsonDoc3 = documentList.get(1);
    Document jsonDoc1 = documentList.get(2);
    Document jsonDoc4 = documentList.get(4);
    Document normalDoc = documentList.get(3);

    assertEquals("prefix1", jsonDoc1.getId());
    assertEquals("prefix2", jsonDoc2.getId());
    assertEquals("prefix3", jsonDoc3.getId());
    assertEquals("prefix4", jsonDoc4.getId());
    assertEquals("12", normalDoc.getString("file_size_bytes"));
    assertEquals("Hello World!", new String(normalDoc.getBytes("file_content")));
    connector.close();
  }

  @Ignore
  @Test
  public void testHandleTarJsonFiles() throws Exception {
    Config config = ConfigFactory.load("FileConnectorTest/testHandleTar.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");
    Connector connector = new FileConnector(config);

    connector.execute(publisher);
    List<Document> documentList = messenger.getDocsSentForProcessing();
    // assert that all documents have been processed
    Assert.assertEquals(5, documentList.size());

    Document jsonDoc2 = documentList.get(0);
    Document jsonDoc3 = documentList.get(1);
    Document jsonDoc1 = documentList.get(2);
    Document jsonDoc4 = documentList.get(4);
    Document normalDoc = documentList.get(3);

    assertEquals("prefix1", jsonDoc1.getId());
    assertEquals("prefix2", jsonDoc2.getId());
    assertEquals("prefix3", jsonDoc3.getId());
    assertEquals("prefix4", jsonDoc4.getId());
    assertEquals("12", normalDoc.getString("file_size_bytes"));
    assertEquals("Hello World!", new String(normalDoc.getBytes("file_content")));
    connector.close();
  }

  @Ignore
  @Test
  public void testHandleGzFiles() throws Exception {
    Config config = ConfigFactory.load("FileConnectorTest/testHandleGz.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");
    Connector connector = new FileConnector(config);

    connector.execute(publisher);
    List<Document> documentList = messenger.getDocsSentForProcessing();
    // one from normal txt, one gzipped txt, and two from jsonl.gz
    assertEquals(4, documentList.size());
    Document txtGzDoc = documentList.get(0);
    Document txtDoc = documentList.get(1);
    Document jsonlGzDoc1 = documentList.get(2);
    Document jsonlGzDoc2 = documentList.get(3);

    assertEquals("12", txtDoc.getString("file_size_bytes"));
    assertEquals("hello World!", new String(txtDoc.getBytes("file_content")));
    assertEquals("37", txtGzDoc.getString("file_size_bytes"));
    assertEquals("hello World! This is the txt gz file.", new String(txtGzDoc.getBytes("file_content")));
    assertEquals("prefix2", jsonlGzDoc1.getId());
    assertEquals("prefix3", jsonlGzDoc2.getId());
    connector.close();
  }

  @Ignore
  @Test
  public void testHandleTarGzFiles() throws Exception {
    Config config = ConfigFactory.load("FileConnectorTest/testHandleTarGz.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");
    Connector connector = new FileConnector(config);

    connector.execute(publisher);
    List<Document> documentList = messenger.getDocsSentForProcessing();

    // assert that all documents have been processed
    Assert.assertEquals(5, documentList.size());

    Document jsonDoc2 = documentList.get(0);
    Document jsonDoc3 = documentList.get(1);
    Document jsonDoc1 = documentList.get(2);
    Document jsonDoc4 = documentList.get(4);
    Document normalDoc = documentList.get(3);

    assertEquals("prefix1", jsonDoc1.getId());
    assertEquals("prefix2", jsonDoc2.getId());
    assertEquals("prefix3", jsonDoc3.getId());
    assertEquals("prefix4", jsonDoc4.getId());
    assertEquals("12", normalDoc.getString("file_size_bytes"));
    assertEquals("Hello World!", new String(normalDoc.getBytes("file_content")));
    connector.close();
  }
}