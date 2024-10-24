package com.kmwllc.lucille.connector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import java.nio.file.spi.FileSystemProvider;

public class FileConnectorTest {

  FileSystem mockFileSystem;

  @Before
  public void setUp() throws Exception {
    mockFileSystem = mock(FileSystem.class);
    when(mockFileSystem.getPath(any())).thenReturn(mock(Path.class));

  }

  @Test
  public void testRetrieveCorrectFileSystem() throws Exception {

    Map<String, Object> configMapG = new HashMap<>();
    configMapG.put("pipeline", "pipeline1");
    configMapG.put("name", "name");
    configMapG.put("pathToStorage", "gs://bucket-name");
    Config configG = ConfigFactory.parseMap(configMapG);
    TestMessenger messengerG = new TestMessenger();
    Publisher publisherG = new PublisherImpl(configG, messengerG, "run", "pipeline1");

    Map<String, Object> configMapA = new HashMap<>();
    configMapA.put("pipeline", "pipeline1");
    configMapA.put("name", "name");
    configMapA.put("pathToStorage", "s3://bucket-name");
    Config configA = ConfigFactory.parseMap(configMapA);
    TestMessenger messengerA = new TestMessenger();
    Publisher publisherA = new PublisherImpl(configA, messengerA, "run", "pipeline1");

    Map<String, Object> configMapAz = new HashMap<>();
    configMapAz.put("pipeline", "pipeline1");
    configMapAz.put("name", "name");
    configMapAz.put("pathToStorage", "azb://bucket-name");
    Config configAz = ConfigFactory.parseMap(configMapAz);
    TestMessenger messengerAz = new TestMessenger();
    Publisher publisherAz = new PublisherImpl(configAz, messengerAz, "run", "pipeline1");

    Map<String, Object> configMapLocal = new HashMap<>();
    configMapLocal.put("pipeline", "pipeline1");
    configMapLocal.put("name", "name");
    configMapLocal.put("pathToStorage", "/User/Path/To/Dir");
    Config configLocal = ConfigFactory.parseMap(configMapLocal);
    TestMessenger messengerLocal = new TestMessenger();
    Publisher publisherLocal = new PublisherImpl(configAz, messengerLocal, "run", "pipeline1");

    FileConnector fileConnectorG = new FileConnector(configG);
    FileConnector fileConnectorA = new FileConnector(configA);
    FileConnector fileConnectorAz = new FileConnector(configAz);
    FileConnector fileConnectorLocal = new FileConnector(configLocal);

    try(MockedStatic<FileSystems> mockFileSystems = mockStatic(FileSystems.class);
        FileSystem mockFileSystem = mock(FileSystem.class)) {
      // when fileSystem calls getPath, we have already gotten file system, so just throw error to escape operation
      when(mockFileSystem.getPath(any())).thenThrow(InvalidPathException.class);
      FileSystemProvider localMockProvider = mock(FileSystemProvider.class);
      when(localMockProvider.getScheme()).thenReturn("gs").thenReturn("s3").thenReturn("azb").thenReturn("file");
      when(mockFileSystem.provider()).thenReturn(localMockProvider);

      mockFileSystems.when(() -> FileSystems.newFileSystem((URI) any(), any())).thenReturn(mockFileSystem);
      mockFileSystems.when(() -> FileSystems.getDefault()).thenReturn(mockFileSystem);
      assertThrows(ConnectorException.class, () -> fileConnectorG.execute(publisherG));
      assertThrows(ConnectorException.class, () -> fileConnectorA.execute(publisherA));
      assertThrows(ConnectorException.class, () -> fileConnectorAz.execute(publisherAz));
      assertThrows(ConnectorException.class, () -> fileConnectorLocal.execute(publisherLocal));

      ArgumentCaptor<URI> argumentCaptor = ArgumentCaptor.forClass(URI.class);
      mockFileSystems.verify(() -> FileSystems.newFileSystem(argumentCaptor.capture(), any()), times(3));

      assertEquals(URI.create("gs://bucket-name"), argumentCaptor.getAllValues().get(0));
      assertEquals(URI.create("s3://bucket-name"), argumentCaptor.getAllValues().get(1));
      assertEquals(URI.create("azb://bucket-name"), argumentCaptor.getAllValues().get(2));

      mockFileSystems.verify(() -> FileSystems.getDefault(), times(1));
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