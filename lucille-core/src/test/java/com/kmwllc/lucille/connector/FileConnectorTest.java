package com.kmwllc.lucille.connector;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.connector.cloudstorageclients.CloudStorageClient;
import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.Arrays;
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
  public void testExecuteForCloudFiles() throws Exception {
    Config config = ConfigFactory.load("FileConnectorTest/gcloudtraversal.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");
    Connector connector = new FileConnector(config);

    try (MockedStatic<CloudStorageClient> mockCloudStorageClient = mockStatic(CloudStorageClient.class)) {
      CloudStorageClient cloudStorageClient = mock(CloudStorageClient.class);
      mockCloudStorageClient.when(() -> CloudStorageClient.getClient(any(), any(), any(), any(), any(), any()))
          .thenReturn(cloudStorageClient);

      connector.execute(publisher);
      verify(cloudStorageClient, times(1)).init();
      verify(cloudStorageClient, times(1)).publishFiles();
    }
  }

  @Test
  public void testValidateGCloudOptions() throws Exception {
    CloudStorageClient cloudStorageClient = mock(CloudStorageClient.class);
    Config config = ConfigFactory.load("FileConnectorTest/gcloudtraversal.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");
    Connector connector = new FileConnector(config);

    Config badConfig = ConfigFactory.load("FileConnectorTest/gcloudtraversalbad.conf");
    TestMessenger badMessenger = new TestMessenger();
    Publisher badPublisher = new PublisherImpl(badConfig, badMessenger, "run", "pipeline1");
    Connector badConnector = new FileConnector(badConfig);


    try (MockedStatic<CloudStorageClient> mockCloudStorageClient = mockStatic(CloudStorageClient.class)) {
      mockCloudStorageClient.when(() -> CloudStorageClient.getClient(any(), any(), any(), any(), any(), any()))
          .thenReturn(cloudStorageClient);

      connector.execute(publisher);
      verify(cloudStorageClient, times(1)).init();
      verify(cloudStorageClient, times(1)).publishFiles();

      assertThrows(IllegalArgumentException.class, () -> badConnector.execute(badPublisher));
    }
  }

  @Test
  public void testValidateS3CloudOptions() throws Exception {
    CloudStorageClient cloudStorageClient = mock(CloudStorageClient.class);
    Config config = ConfigFactory.load("FileConnectorTest/s3cloudtraversal.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");
    Connector connector = new FileConnector(config);

    Config badConfig = ConfigFactory.load("FileConnectorTest/s3cloudtraversalbad.conf");
    TestMessenger badMessenger = new TestMessenger();
    Publisher badPublisher = new PublisherImpl(badConfig, badMessenger, "run", "pipeline1");
    Connector badConnector = new FileConnector(badConfig);


    try (MockedStatic<CloudStorageClient> mockCloudStorageClient = mockStatic(CloudStorageClient.class)) {
      mockCloudStorageClient.when(() -> CloudStorageClient.getClient(any(), any(), any(), any(), any(), any()))
          .thenReturn(cloudStorageClient);

      connector.execute(publisher);
      verify(cloudStorageClient, times(1)).init();
      verify(cloudStorageClient, times(1)).publishFiles();

      assertThrows(IllegalArgumentException.class, () -> badConnector.execute(badPublisher));
    }
  }

  @Test
  public void testValidateAzureCloudOptions() throws Exception {
    CloudStorageClient cloudStorageClient = mock(CloudStorageClient.class);
    Config config = ConfigFactory.load("FileConnectorTest/azbcloudtraversal.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");
    Connector connector = new FileConnector(config);

    Config badConfig = ConfigFactory.load("FileConnectorTest/azbcloudtraversalbad.conf");
    TestMessenger badMessenger = new TestMessenger();
    Publisher badPublisher = new PublisherImpl(badConfig, badMessenger, "run", "pipeline1");
    Connector badConnector = new FileConnector(badConfig);


    try (MockedStatic<CloudStorageClient> mockCloudStorageClient = mockStatic(CloudStorageClient.class)) {
      mockCloudStorageClient.when(() -> CloudStorageClient.getClient(any(), any(), any(), any(), any(), any()))
          .thenReturn(cloudStorageClient);

      connector.execute(publisher);
      verify(cloudStorageClient, times(1)).init();
      verify(cloudStorageClient, times(1)).publishFiles();

      assertThrows(IllegalArgumentException.class, () -> badConnector.execute(badPublisher));
    }
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
  }
}