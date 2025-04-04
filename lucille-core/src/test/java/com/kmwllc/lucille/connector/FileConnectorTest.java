package com.kmwllc.lucille.connector;

import static com.kmwllc.lucille.connector.FileConnector.FILE_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/gcloudtraversal.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");
    Connector connector = new FileConnector(config);

    try (MockedStatic<StorageClient> mockCloudStorageClient = mockStatic(StorageClient.class)) {
      StorageClient storageClient = mock(StorageClient.class);
      mockCloudStorageClient.when(() -> StorageClient.create(any(), any()))
          .thenReturn(storageClient);

      connector.execute(publisher);
      verify(storageClient, times(1)).init();
      verify(storageClient, times(1)).traverse(any(Publisher.class), any(TraversalParams.class));
      verify(storageClient, times(1)).shutdown();
    }
  }

  @Test
  public void testExecuteSuccessfulExtraCloudConfig() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/multipleCloud.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");
    Connector connector = new FileConnector(config);

    try (MockedStatic<StorageClient> mockCloudStorageClient = mockStatic(StorageClient.class)) {
      StorageClient storageClient = mock(StorageClient.class);
      mockCloudStorageClient.when(() -> StorageClient.create(any(), any()))
          .thenReturn(storageClient);

      connector.execute(publisher);
      verify(storageClient, times(1)).init();
      verify(storageClient, times(1)).traverse(any(Publisher.class), any(TraversalParams.class));
      verify(storageClient, times(1)).shutdown();
    }
  }

  @Test
  public void testInitFailed() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/gcloudtraversal.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");
    Connector connector = new FileConnector(config);

    try (MockedStatic<StorageClient> mockCloudStorageClient = mockStatic(StorageClient.class)) {
      StorageClient storageClient = mock(StorageClient.class);
      mockCloudStorageClient.when(() -> StorageClient.create(any(), any()))
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
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/gcloudtraversal.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");
    Connector connector = new FileConnector(config);

    try (MockedStatic<StorageClient> mockCloudStorageClient = mockStatic(StorageClient.class)) {
      StorageClient storageClient = mock(StorageClient.class);
      mockCloudStorageClient.when(() -> StorageClient.create(any(), any()))
          .thenReturn(storageClient);

      // the try catch block in FileConnector will catch any Exception class and throw a ConnectorException
      doThrow(new Exception("Failed to publish files")).when(storageClient).traverse(any(Publisher.class), any(TraversalParams.class));
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
}