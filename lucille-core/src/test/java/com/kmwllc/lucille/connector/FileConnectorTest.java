package com.kmwllc.lucille.connector;

import static com.kmwllc.lucille.connector.FileConnector.FILE_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.connector.storageclient.StorageClient;
import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
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
  public void testExecuteSuccessful() throws Exception {
    Config config = ConfigFactory.load("FileConnectorTest/gcloudtraversal.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");
    Connector connector = new FileConnector(config);

    try (MockedStatic<StorageClient> mockCloudStorageClient = mockStatic(StorageClient.class)) {
      StorageClient storageClient = mock(StorageClient.class);
      mockCloudStorageClient.when(() -> StorageClient.create(any(), any(), any(), any(), any(), any()))
          .thenReturn(storageClient);

      connector.execute(publisher);
      verify(storageClient, times(1)).init();
      verify(storageClient, times(1)).traverse(publisher);
      verify(storageClient, times(1)).shutdown();
    }
  }

  @Test
  public void testInitFailed() throws Exception {
    Config config = ConfigFactory.load("FileConnectorTest/gcloudtraversal.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");
    Connector connector = new FileConnector(config);

    try (MockedStatic<StorageClient> mockCloudStorageClient = mockStatic(StorageClient.class)) {
      StorageClient storageClient = mock(StorageClient.class);
      mockCloudStorageClient.when(() -> StorageClient.create(any(), any(), any(), any(), any(), any()))
          .thenReturn(storageClient);

      // init method did not declare to throw any Exception, so using RuntimeException
      // the try catch block in FileConnector will catch any Exception class and throw a ConnectorException
      doThrow(new RuntimeException("Failed to initialize client")).when(storageClient).init();
      assertThrows(ConnectorException.class, () -> connector.execute(publisher));
      // verify that shutdown is called even gettingClient fails
      verify(storageClient, times(1)).shutdown();
    }
  }

  @Test
  public void testPublishFilesFailed() throws Exception {
    Config config = ConfigFactory.load("FileConnectorTest/gcloudtraversal.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");
    Connector connector = new FileConnector(config);

    try (MockedStatic<StorageClient> mockCloudStorageClient = mockStatic(StorageClient.class)) {
      StorageClient storageClient = mock(StorageClient.class);
      mockCloudStorageClient.when(() -> StorageClient.create(any(), any(), any(), any(), any(), any()))
          .thenReturn(storageClient);

      // the try catch block in FileConnector will catch any Exception class and throw a ConnectorException
      doThrow(new Exception("Failed to publish files")).when(storageClient).traverse(publisher);
      assertThrows(ConnectorException.class, () -> connector.execute(publisher));
      // verify that shutdown is called even gettingClient fails
      verify(storageClient, times(1)).shutdown();
    }
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
      tempDir.delete();
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
      tempDir.delete();
    }
  }

  @Test
  public void testExampleTraversal() throws Exception {
    Config config = ConfigFactory.load("FileConnectorTest/example.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");
    Connector connector = new FileConnector(config);

    connector.execute(publisher);
    List<Document> documentList = messenger.getDocsSentForProcessing();

    // assert that all documents have been processed
    Assert.assertEquals(16, documentList.size());

    Document doc1 = documentList.get(0);
    assertEquals("jsonHandled-a", doc1.getId());
    assertEquals("Rustic Cows Mug", doc1.getString("name"));

    // compressed, but non-archived, file
    Document doc2 = documentList.get(1);
    assertTrue(doc2.getId().startsWith("normal-"));
    assertTrue(doc2.getString(FILE_PATH).endsWith("helloWorld.txt.gz" + File.pathSeparatorChar + "helloWorld.txt"));

    Document doc3 = documentList.get(2);
    assertTrue(doc3.getId().startsWith("normal-"));
    assertTrue(doc3.getString(FILE_PATH).endsWith("subDirWith2TxtFiles.zip!subDirWith2TxtFiles/first.txt"));
    assertEquals("First!", new String(doc3.getBytes("file_content")));

    Document doc4 = documentList.get(3);
    assertTrue(doc4.getId().startsWith("normal-"));
    assertTrue(doc4.getString(FILE_PATH).endsWith("subDirWith2TxtFiles.zip!subDirWith2TxtFiles/second.txt"));
    assertEquals("Second!", new String(doc4.getBytes("file_content")));

    Document doc5 = documentList.get(4);
    assertEquals("jsonHandled-b", doc5.getId());
    assertEquals("Gorgeous Woman Mug", doc5.getString("name"));

    Document doc6 = documentList.get(5);
    assertEquals("jsonHandled-c1", doc6.getId());
    assertEquals("Awesome Night Mug", doc6.getString("name"));

    Document doc7 = documentList.get(6);
    assertEquals("jsonHandled-c2", doc7.getId());
    assertEquals("Ergonomic Mountains Mug", doc7.getString("name"));

    Document doc8 = documentList.get(7);
    assertEquals("jsonHandled-c3", doc8.getId());
    assertEquals("Refined Fog Mug", doc8.getString("name"));

    Document doc9 = documentList.get(8);
    assertEquals("jsonHandled-c4", doc9.getId());
    assertEquals("Sleek Castle Mug", doc9.getString("name"));

    Document doc10 = documentList.get(9);
    assertEquals("jsonHandled-c5", doc10.getId());
    assertEquals("Small City Mug", doc10.getString("name"));

    Document doc11 = documentList.get(10);
    assertTrue(doc11.getId().startsWith("normal-"));
    assertTrue(doc11.getString(FILE_PATH).endsWith("subdir/e.yaml"));

    Document doc12 = documentList.get(11);
    assertEquals("csvHandled-default.csv-1", doc12.getId());
    assertTrue(doc12.getString("source").endsWith("subdirWith1csv1xml.tar.gz!subdirWith1csv1xml/default.csv"));

    Document doc13 = documentList.get(12);
    assertEquals("csvHandled-default.csv-2", doc13.getId());
    assertTrue(doc13.getString("source").endsWith("subdirWith1csv1xml.tar.gz!subdirWith1csv1xml/default.csv"));

    Document doc14 = documentList.get(13);
    assertEquals("csvHandled-default.csv-3", doc14.getId());
    assertTrue(doc14.getString("source").endsWith("subdirWith1csv1xml.tar.gz!subdirWith1csv1xml/default.csv"));

    Document doc15 = documentList.get(14);
    assertEquals("xmlHandled-1001", doc15.getId());
    assertEquals("<staff>\n" +
        "        <id>1001</id>\n" +
        "        <name>daniel</name>\n" +
        "        <role>software engineer</role>\n" +
        "        <salary currency=\"USD\">3000</salary>\n" +
        "        <bio>I am from San Diego</bio>\n" +
        "    </staff>", doc15.getString("xml"));

    Document doc16 = documentList.get(15);
    assertEquals("xmlHandled-1002", doc16.getId());
    assertEquals("<staff>\n" +
        "        <id>1002</id>\n" +
        "        <name>brian</name>\n" +
        "        <role>admin</role>\n" +
        "        <salary currency=\"EUR\">8000</salary>\n" +
        "        <bio>I enjoy reading</bio>\n" +
        "    </staff>", doc16.getString("xml"));
  }
}