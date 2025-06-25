package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.ARCHIVE_FILE_SEPARATOR;
import static com.kmwllc.lucille.connector.FileConnector.CONTENT;
import static com.kmwllc.lucille.connector.FileConnector.FILE_PATH;
import static com.kmwllc.lucille.connector.FileConnector.GET_FILE_CONTENT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.specialized.BlobInputStream;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.core.fileHandler.FileHandler;
import com.kmwllc.lucille.core.fileHandler.JsonFileHandler;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.ByteArrayInputStream;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.net.URI;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class AzureStorageClientTest {

  @Test
  public void testInit() throws Exception{
    Config cloudOptions = ConfigFactory.parseMap(Map.of("connectionString", "connectionString"));

    AzureStorageClient azureStorageClient = new AzureStorageClient(cloudOptions);

    try (MockedConstruction<BlobServiceClientBuilder> builder = Mockito.mockConstruction(BlobServiceClientBuilder.class, (mock,context) -> {
      when(mock.connectionString(anyString())).thenReturn(mock);
      when(mock.buildClient()).thenReturn(mock(BlobServiceClient.class));
    })) {
      azureStorageClient.init();
      verify(builder.constructed().get(0), times(1)).connectionString("connectionString");
    }

    cloudOptions = ConfigFactory.parseMap(Map.of("accountName", "accountName",
        "accountKey", "accountKey"));
    azureStorageClient = new AzureStorageClient(cloudOptions);

    try (MockedConstruction<BlobServiceClientBuilder> builder = Mockito.mockConstruction(BlobServiceClientBuilder.class, (mock,context) -> {
      when(mock.credential((StorageSharedKeyCredential) any())).thenReturn(mock);
      when(mock.buildClient()).thenReturn(mock(BlobServiceClient.class));
    })) {
      azureStorageClient.init();
      verify(builder.constructed().get(0), times(1)).credential(any(StorageSharedKeyCredential.class));
    }

    azureStorageClient.shutdown();
  }


  @Test
  public void testPublishValidFiles() throws Exception {
    Config cloudOptions = ConfigFactory.parseMap(Map.of("connectionString", "connectionString"));
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    AzureStorageClient azureStorageClient = new AzureStorageClient(cloudOptions);
    TraversalParams params = new TraversalParams(ConfigFactory.empty(), URI.create("https://storagename.blob.core.windows.net/folder/"), "prefix-");

    BlobServiceClient mockServiceClient = mock(BlobServiceClient.class);
    BlobContainerClient mockClient = mock(BlobContainerClient.class, RETURNS_DEEP_STUBS);
    PagedIterable<BlobItem> pagedIterable = mock(PagedIterable.class);
    when(pagedIterable.stream()).thenReturn(getBlobItemStream());
    when(mockClient.listBlobs(any(), any())).thenReturn(pagedIterable);

    when(mockServiceClient.getBlobContainerClient(any())).thenReturn(mockClient);
    azureStorageClient.setServiceClientForTesting(mockServiceClient);
    when(mockClient.getBlobClient(anyString()).downloadContent().toBytes())
        .thenReturn(new byte[]{1, 2, 3, 4}) // blob1
        .thenReturn(new byte[]{5, 6, 7, 8}) // blob2
        .thenReturn(new byte[]{9, 10, 11, 12}) // blob3
        .thenReturn(new byte[]{13, 14, 15, 16}); // blob4

    azureStorageClient.initializeForTesting();
    azureStorageClient.traverse(publisher, params);

    List<Document> documents = messenger.getDocsSentForProcessing();

    // all documents processed
    assertEquals(4, documents.size());
    Document doc1 = documents.get(0);
    assertTrue(doc1.getId().startsWith("prefix-"));
    assertEquals("https://storagename.blob.core.windows.net/folder/blob1", doc1.getString(FILE_PATH));
    assertEquals("1", doc1.getString("file_size_bytes"));
    assertArrayEquals(new byte[]{1, 2, 3, 4}, doc1.getBytes(CONTENT));

    Document doc2 = documents.get(1);
    assertTrue(doc2.getId().startsWith("prefix-"));
    assertEquals("https://storagename.blob.core.windows.net/folder/blob2", doc2.getString(FILE_PATH));
    assertEquals("2", doc2.getString("file_size_bytes"));
    assertArrayEquals(new byte[]{5, 6, 7, 8}, doc2.getBytes(CONTENT));

    Document doc3 = documents.get(2);
    assertTrue(doc3.getId().startsWith("prefix-"));
    assertEquals("https://storagename.blob.core.windows.net/folder/blob3", doc3.getString(FILE_PATH));
    assertEquals("3", doc3.getString("file_size_bytes"));
    assertArrayEquals(new byte[]{9, 10, 11, 12}, doc3.getBytes(CONTENT));

    Document doc4 = documents.get(3);
    assertTrue(doc4.getId().startsWith("prefix-"));
    assertEquals("https://storagename.blob.core.windows.net/folder/blob4", doc4.getString(FILE_PATH));
    assertEquals("4", doc4.getString("file_size_bytes"));
    assertArrayEquals(new byte[]{13, 14, 15, 16}, doc4.getBytes(CONTENT));
  }

  @Test
  public void testSkipFileContent() throws Exception {
    Config cloudOptions = ConfigFactory.parseMap(Map.of("connectionString", "connectionString"));
    TestMessenger messenger = new TestMessenger();
    Config connectorConfig = ConfigFactory.parseMap(Map.of(
        "fileOptions", Map.of(GET_FILE_CONTENT, false)
    ));
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    AzureStorageClient azureStorageClient = new AzureStorageClient(cloudOptions);
    TraversalParams params = new TraversalParams(connectorConfig, URI.create("https://storagename.blob.core.windows.net/folder/"), "prefix-");

    BlobContainerClient mockClient = mock(BlobContainerClient.class, RETURNS_DEEP_STUBS);
    PagedIterable<BlobItem> pagedIterable = mock(PagedIterable.class);

    BlobItem blobItem1 = new BlobItem();
    blobItem1.setName("blob1");
    blobItem1.setProperties(new BlobItemProperties()
        .setCreationTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(1), ZoneId.of("UTC")))
        .setLastModified(OffsetDateTime.ofInstant(Instant.ofEpochMilli(1), ZoneId.of("UTC")))
        .setContentLength(1L));

    when(pagedIterable.stream()).thenReturn(Stream.of(blobItem1));
    when(mockClient.listBlobs(any(), any())).thenReturn(pagedIterable);

    BlobServiceClient mockServiceClient = mock(BlobServiceClient.class);
    when(mockServiceClient.getBlobContainerClient(any())).thenReturn(mockClient);
    azureStorageClient.setServiceClientForTesting(mockServiceClient);

    when(mockClient.getBlobClient(anyString()).downloadContent().toBytes())
        .thenReturn(new byte[]{1, 2, 3, 4});

    azureStorageClient.initializeForTesting();
    azureStorageClient.traverse(publisher, params);

    List<Document> documents = messenger.getDocsSentForProcessing();

    // only 1 blob
    assertEquals(1, documents.size());
    // no fileContent
    assertNull(documents.get(0).getBytes(CONTENT));
  }

  @Test
  public void testPublishInvalidFiles() throws Exception {
    Config cloudOptions = ConfigFactory.parseMap(Map.of("connectionString", "connectionString"));
    TestMessenger messenger = new TestMessenger();
    Config connectorConfig = ConfigFactory.parseMap(Map.of(
        "filterOptions", Map.of("excludes", List.of("blob3", "blob4"))
    ));
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    AzureStorageClient azureStorageClient = new AzureStorageClient(cloudOptions);
    TraversalParams params = new TraversalParams(connectorConfig, URI.create("https://storagename.blob.core.windows.net/folder/"), "prefix-");

    BlobContainerClient mockClient = mock(BlobContainerClient.class, RETURNS_DEEP_STUBS);
    PagedIterable<BlobItem> pagedIterable = mock(PagedIterable.class);
    when(pagedIterable.stream()).thenReturn(getBlobItemStreamWithDirectory());
    when(mockClient.listBlobs(any(), any())).thenReturn(pagedIterable);

    BlobServiceClient mockServiceClient = mock(BlobServiceClient.class);
    when(mockServiceClient.getBlobContainerClient(any())).thenReturn(mockClient);
    azureStorageClient.setServiceClientForTesting(mockServiceClient);

    when(mockClient.getBlobClient(anyString()).downloadContent().toBytes())
        .thenReturn(new byte[]{1, 2, 3, 4}) // blob1
        .thenReturn(new byte[]{5, 6, 7, 8}) // blob2
        .thenReturn(new byte[]{9, 10, 11, 12}) // blob3
        .thenReturn(new byte[]{13, 14, 15, 16}); // blob4

    azureStorageClient.initializeForTesting();
    azureStorageClient.traverse(publisher, params);

    List<Document> documents = messenger.getDocsSentForProcessing();

    // only blob1 processed due to blob2 being a directory and blob3 and blob4 being excluded via regex
    assertEquals(1, documents.size());
    Document doc1 = documents.get(0);
    assertTrue(doc1.getId().startsWith("prefix-"));
    assertEquals("https://storagename.blob.core.windows.net/folder/blob1", doc1.getString(FILE_PATH));
    assertEquals("1", doc1.getString("file_size_bytes"));
    assertArrayEquals(new byte[]{1, 2, 3, 4}, doc1.getBytes(CONTENT));
  }

  @Test
  public void testPublishUsingFileHandler() throws Exception {
    Config cloudOptions = ConfigFactory.parseMap(Map.of("connectionString", "connectionString"));
    TestMessenger messenger = new TestMessenger();
    Config connectorConfig = ConfigFactory.parseMap(Map.of(
        "fileOptions", Map.of(GET_FILE_CONTENT, false),
        "fileHandlers", Map.of("json", Map.of())
    ));
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    // azure storage client that handles json files
    AzureStorageClient azureStorageClient = new AzureStorageClient(cloudOptions);

    BlobContainerClient mockClient = mock(BlobContainerClient.class, RETURNS_DEEP_STUBS);
    PagedIterable<BlobItem> pagedIterable = mock(PagedIterable.class);
    when(pagedIterable.stream()).thenReturn(getJsonBlobItemStream());
    when(mockClient.listBlobs(any(), any())).thenReturn(pagedIterable);

    BlobServiceClient mockServiceClient = mock(BlobServiceClient.class);
    when(mockServiceClient.getBlobContainerClient(any())).thenReturn(mockClient);
    azureStorageClient.setServiceClientForTesting(mockServiceClient);

    try (MockedStatic<FileHandler> mockFileHandler = mockStatic(FileHandler.class)) {
      FileHandler jsonFileHandler = mock(JsonFileHandler.class);
      mockFileHandler.when(() -> FileHandler.createFromConfig(any())).thenReturn(Map.of("json", jsonFileHandler));

      TraversalParams params = new TraversalParams(connectorConfig, URI.create("https://storagename.blob.core.windows.net/folder/"), "prefix-");

      azureStorageClient.initializeForTesting();
      azureStorageClient.traverse(publisher, params);
      // verify that the processFileAndPublish is only called for the json files
      ArgumentCaptor<String> fileNameCaptor = ArgumentCaptor.forClass(String.class);
      verify(jsonFileHandler, times(2)).processFileAndPublish(any(), any(), fileNameCaptor.capture());
      List<String> fileNames = fileNameCaptor.getAllValues();
      assertEquals("https://storagename.blob.core.windows.net/folder/blob1.json", fileNames.get(0));
      assertEquals("https://storagename.blob.core.windows.net/folder/blob2.json", fileNames.get(1));
    }

    // shutdown client and handlers
    azureStorageClient.shutdown();
  }

  @Test
  public void testPublishCompressedAndArchived() throws Exception {
    Config cloudOptions = ConfigFactory.parseMap(Map.of("connectionString", "connectionString"));
    TestMessenger messenger = new TestMessenger();
    Config connectorConfig = ConfigFactory.parseMap(Map.of(
        "fileOptions", Map.of(
            "handleArchivedFiles", true,
            "handleCompressedFiles", true
        ),
        "fileHandlers", Map.of(
            "csv", Map.of(),
            "json", Map.of()
        )
    ));
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    AzureStorageClient azureStorageClient = new AzureStorageClient(cloudOptions);
    TraversalParams params = new TraversalParams(connectorConfig, URI.create("https://storagename.blob.core.windows.net/folder/"), "prefix-");

    BlobContainerClient mockClient = mock(BlobContainerClient.class, RETURNS_DEEP_STUBS);
    PagedIterable<BlobItem> pagedIterable = mock(PagedIterable.class);
    when(pagedIterable.stream()).thenReturn(getCompressedAndArchivedBlobStream());
    when(mockClient.listBlobs(any(), any())).thenReturn(pagedIterable);

    BlobServiceClient mockServiceClient = mock(BlobServiceClient.class);
    when(mockServiceClient.getBlobContainerClient(any())).thenReturn(mockClient);
    azureStorageClient.setServiceClientForTesting(mockServiceClient);

    String folderPath = new File("src/test/resources/StorageClientTest/testCompressedAndArchived").getAbsolutePath();
    List<BlobInputStream> mockedStreams = buildMockStreamList(folderPath);

    when(mockClient.getBlobClient(anyString()).openInputStream())
        .thenReturn(mockedStreams.get(0))
        .thenReturn(mockedStreams.get(1))
        .thenReturn(mockedStreams.get(2))
        .thenReturn(mockedStreams.get(3))
        .thenReturn(mockedStreams.get(4))
        .thenReturn(mockedStreams.get(5));

    azureStorageClient.initializeForTesting();
    azureStorageClient.traverse(publisher, params);

    List<Document> docs = messenger.getDocsSentForProcessing();

    assertEquals(19, docs.size());

    // check documents published from jsonlCsvAndFolderWithFooTxt.tar
    Document doc1 = docs.get(0);
    assertEquals("default.csv-1", doc1.getId());
    assertEquals("https://storagename.blob.core.windows.net/folder/jsonlCsvAndFolderWithFooTxt.tar"
        + ARCHIVE_FILE_SEPARATOR + "default.csv", doc1.getString("source"));
    Document doc2 = docs.get(1);
    assertEquals("default.csv-2", doc2.getId());
    assertEquals("https://storagename.blob.core.windows.net/folder/jsonlCsvAndFolderWithFooTxt.tar"
        + ARCHIVE_FILE_SEPARATOR + "default.csv", doc2.getString("source"));
    Document doc3 = docs.get(2);
    assertEquals("default.csv-3", doc3.getId());
    assertEquals("https://storagename.blob.core.windows.net/folder/jsonlCsvAndFolderWithFooTxt.tar"
        + ARCHIVE_FILE_SEPARATOR + "default.csv", doc3.getString("source"));
    Document doc4 = docs.get(3);
    assertEquals("2", doc4.getId());
    assertEquals("Gorgeous Woman Mug", doc4.getString("name"));
    Document doc5 = docs.get(4);
    assertEquals("3", doc5.getId());
    assertEquals("Awesome City Mug", doc5.getString("name"));
    Document doc6 = docs.get(5);
    assertEquals("https://storagename.blob.core.windows.net/folder/jsonlCsvAndFolderWithFooTxt.tar"
        + ARCHIVE_FILE_SEPARATOR + "FolderWithFooTxt/foo.txt", doc6.getString(FILE_PATH));

    // check documents published from textFiles.tar
    Document doc7 = docs.get(6);
    assertEquals("https://storagename.blob.core.windows.net/folder/textFiles.tar" + ARCHIVE_FILE_SEPARATOR + "helloWorld.txt", doc7.getString(FILE_PATH));
    Document doc8 = docs.get(7);
    assertEquals("https://storagename.blob.core.windows.net/folder/textFiles.tar" + ARCHIVE_FILE_SEPARATOR + "goodbye.txt", doc8.getString(FILE_PATH));

    // check documents published from jsonlCsvAndFolderWithFooTxt.tar.gz
    Document doc9 = docs.get(8);
    assertEquals("default.csv-1", doc9.getId());
    assertEquals("https://storagename.blob.core.windows.net/folder/jsonlCsvAndFolderWithFooTxt.tar.gz"
        + ARCHIVE_FILE_SEPARATOR + "default.csv", doc9.getString("source"));
    Document doc10 = docs.get(9);
    assertEquals("default.csv-2", doc10.getId());
    assertEquals("https://storagename.blob.core.windows.net/folder/jsonlCsvAndFolderWithFooTxt.tar.gz"
        + ARCHIVE_FILE_SEPARATOR + "default.csv", doc10.getString("source"));
    Document doc11 = docs.get(10);
    assertEquals("default.csv-3", doc11.getId());
    assertEquals("https://storagename.blob.core.windows.net/folder/jsonlCsvAndFolderWithFooTxt.tar.gz"
        + ARCHIVE_FILE_SEPARATOR + "default.csv", doc11.getString("source"));
    Document doc12 = docs.get(11);
    assertEquals("2", doc12.getId());
    assertEquals("Gorgeous Woman Mug", doc12.getString("name"));
    Document doc13 = docs.get(12);
    assertEquals("3", doc13.getId());
    assertEquals("Awesome City Mug", doc13.getString("name"));
    Document doc14 = docs.get(13);
    assertEquals("https://storagename.blob.core.windows.net/folder/jsonlCsvAndFolderWithFooTxt.tar.gz"
        + ARCHIVE_FILE_SEPARATOR + "FolderWithFooTxt/foo.txt", doc14.getString(FILE_PATH));
    // check documents published from zipped.csv.zip
    Document doc15 = docs.get(14);
    assertEquals("zipped.csv-1", doc15.getId());
    assertEquals("https://storagename.blob.core.windows.net/folder/zipped.csv.zip"
        + ARCHIVE_FILE_SEPARATOR + "zipped.csv", doc15.getString("source"));
    Document doc16 = docs.get(15);
    assertEquals("zipped.csv-2", doc16.getId());
    assertEquals("https://storagename.blob.core.windows.net/folder/zipped.csv.zip"
        + ARCHIVE_FILE_SEPARATOR + "zipped.csv", doc16.getString("source"));
    Document doc17 = docs.get(16);
    assertEquals("zipped.csv-3", doc17.getId());
    assertEquals("https://storagename.blob.core.windows.net/folder/zipped.csv.zip"
        + ARCHIVE_FILE_SEPARATOR + "zipped.csv", doc17.getString("source"));
    // check documents published from zippedFolder.zip
    Document doc18 = docs.get(17);
    assertEquals("https://storagename.blob.core.windows.net/folder/zippedFolder.zip"
        + ARCHIVE_FILE_SEPARATOR + "zippedFolder/foo.txt", doc18.getString(FILE_PATH));
    // check documents published from hello.zip
    Document doc19 = docs.get(18);
    assertEquals("https://storagename.blob.core.windows.net/folder/hello.zip"
        + ARCHIVE_FILE_SEPARATOR + "hello", doc19.getString(FILE_PATH));

    azureStorageClient.shutdown();
  }

  @Test
  public void testMovingFiles() throws Exception {
    Config cloudOptions = ConfigFactory.parseMap(Map.of("connectionString", "connectionString"));
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Config connectorConfig = ConfigFactory.parseMap(Map.of(
        "fileOptions", Map.of(
            "moveToAfterProcessing", "https://storagename.blob.core.windows.net/container/processed/",
            "getFileContent", false
        )
    ));

    AzureStorageClient azureStorageClient = new AzureStorageClient(cloudOptions);
    TraversalParams params = new TraversalParams(connectorConfig, URI.create("https://storagename.blob.core.windows.net/container/"), "");

    PagedIterable<BlobItem> pagedIterable = mock(PagedIterable.class);
    when(pagedIterable.stream()).thenReturn(getBlobItemStream());

    BlobContainerClient mockContainerClient = mock(BlobContainerClient.class, RETURNS_DEEP_STUBS);
    when(mockContainerClient.listBlobs(any(), any())).thenReturn(pagedIterable);

    BlobServiceClient mockServiceClient = mock(BlobServiceClient.class);
    when(mockServiceClient.getBlobContainerClient(any())).thenReturn(mockContainerClient);
    azureStorageClient.setServiceClientForTesting(mockServiceClient);

    // this is the client ew call beginCopy on
    BlobClient mockSourceBlobClient = mock(BlobClient.class);
    when(mockContainerClient.getBlobClient(eq("blob1"))).thenReturn(mockSourceBlobClient);
    when(mockContainerClient.getBlobClient(eq("blob2"))).thenReturn(mockSourceBlobClient);
    when(mockContainerClient.getBlobClient(eq("blob3"))).thenReturn(mockSourceBlobClient);
    when(mockContainerClient.getBlobClient(eq("blob4"))).thenReturn(mockSourceBlobClient);

    // This is the client we call .getBlobURL() on
    BlobClient mockTargetBlobClient1 = mock(BlobClient.class);
    BlobClient mockTargetBlobClient2 = mock(BlobClient.class);
    BlobClient mockTargetBlobClient3 = mock(BlobClient.class);
    BlobClient mockTargetBlobClient4 = mock(BlobClient.class);

    when(mockContainerClient.getBlobClient("processed/blob1")).thenReturn(mockTargetBlobClient1);
    when(mockContainerClient.getBlobClient("processed/blob2")).thenReturn(mockTargetBlobClient2);
    when(mockContainerClient.getBlobClient("processed/blob3")).thenReturn(mockTargetBlobClient3);
    when(mockContainerClient.getBlobClient("processed/blob4")).thenReturn(mockTargetBlobClient4);

    when(mockTargetBlobClient1.getBlobUrl()).thenReturn("blob1");
    when(mockTargetBlobClient2.getBlobUrl()).thenReturn("blob2");
    when(mockTargetBlobClient3.getBlobUrl()).thenReturn("blob3");
    when(mockTargetBlobClient4.getBlobUrl()).thenReturn("blob4");

    when(mockSourceBlobClient.beginCopy(any(), any())).thenAnswer(invocationOnMock -> {
      String url = invocationOnMock.getArgument(0, String.class);

      assertTrue(url.equals("blob1")
          || url.equals("blob2")
          || url.equals("blob3")
          || url.equals("blob4"));

      return null;
    });

    azureStorageClient.initializeForTesting();
    azureStorageClient.traverse(publisher, params);

    verify(mockSourceBlobClient, times(4)).beginCopy(any(), any());

    azureStorageClient.shutdown();
    assertEquals(4, messenger.getDocsSentForProcessing().size());
  }

  @Test
  public void testMovingFilesAcrossContainers() throws Exception {
    Config cloudOptions = ConfigFactory.parseMap(Map.of("connectionString", "connectionString"));
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Config connectorConfig = ConfigFactory.parseMap(Map.of(
        "fileOptions", Map.of(
            "moveToAfterProcessing", "https://storagename.blob.core.windows.net/container/processed/",
            "getFileContent", false
        )
    ));

    AzureStorageClient azureStorageClient = new AzureStorageClient(cloudOptions);
    TraversalParams params = new TraversalParams(connectorConfig, URI.create("https://storagename.blob.core.windows.net/container/"), "");

    PagedIterable<BlobItem> pagedIterable = mock(PagedIterable.class);
    when(pagedIterable.stream()).thenReturn(getBlobItemStream());

    BlobContainerClient mockContainerClient = mock(BlobContainerClient.class, RETURNS_DEEP_STUBS);
    when(mockContainerClient.listBlobs(any(), any())).thenReturn(pagedIterable);

    BlobContainerClient mockProcessedContainerClient = mock(BlobContainerClient.class, RETURNS_DEEP_STUBS);

    BlobServiceClient mockServiceClient = mock(BlobServiceClient.class);
    when(mockServiceClient.getBlobContainerClient(eq("container"))).thenReturn(mockContainerClient);
    azureStorageClient.setServiceClientForTesting(mockServiceClient);

    // The clients in "container" we call .beginCopy on - using all the same since the mocked method body is the same
    BlobClient mockSourceBlobClient = mock(BlobClient.class);
    when(mockContainerClient.getBlobClient(eq("blob1"))).thenReturn(mockSourceBlobClient);
    when(mockContainerClient.getBlobClient(eq("blob2"))).thenReturn(mockSourceBlobClient);
    when(mockContainerClient.getBlobClient(eq("blob3"))).thenReturn(mockSourceBlobClient);
    when(mockContainerClient.getBlobClient(eq("blob4"))).thenReturn(mockSourceBlobClient);

    // The clients we call .getBlobURL() on
    BlobClient mockProcessedBlobClient1 = mock(BlobClient.class);
    BlobClient mockProcessedBlobClient2 = mock(BlobClient.class);
    BlobClient mockProcessedBlobClient3 = mock(BlobClient.class);
    BlobClient mockProcessedBlobClient4 = mock(BlobClient.class);

    when(mockServiceClient.getBlobContainerClient(eq("processed"))).thenReturn(mockProcessedContainerClient);

    when(mockProcessedContainerClient.getBlobClient("blob1")).thenReturn(mockProcessedBlobClient1);
    when(mockProcessedContainerClient.getBlobClient("blob2")).thenReturn(mockProcessedBlobClient2);
    when(mockProcessedContainerClient.getBlobClient("blob3")).thenReturn(mockProcessedBlobClient3);
    when(mockProcessedContainerClient.getBlobClient("blob4")).thenReturn(mockProcessedBlobClient4);

    when(mockProcessedBlobClient1.getBlobUrl()).thenReturn("blob1");
    when(mockProcessedBlobClient2.getBlobUrl()).thenReturn("blob2");
    when(mockProcessedBlobClient3.getBlobUrl()).thenReturn("blob3");
    when(mockProcessedBlobClient4.getBlobUrl()).thenReturn("blob4");

    when(mockSourceBlobClient.beginCopy(any(), any())).thenAnswer(invocationOnMock -> {
      String url = invocationOnMock.getArgument(0, String.class);

      assertTrue(url.equals("blob1")
          || url.equals("blob2")
          || url.equals("blob3")
          || url.equals("blob4"));

      return null;
    });

    azureStorageClient.initializeForTesting();
    azureStorageClient.traverse(publisher, params);

    verify(mockSourceBlobClient, times(4)).beginCopy(any(), any());

    azureStorageClient.shutdown();
    assertEquals(4, messenger.getDocsSentForProcessing().size());
  }

  @Test
  public void testGetFileContentStream() throws Exception {
    Config cloudOptions = ConfigFactory.parseMap(Map.of("connectionString", "connectionString"));
    AzureStorageClient storageClient = new AzureStorageClient(cloudOptions);
    URI testURI = new URI("https://storagename.blob.core.windows.net/folder/hello.txt");

    BlobServiceClient mockServiceClient = mock(BlobServiceClient.class);
    BlobContainerClient mockContainerClient = mock(BlobContainerClient.class);
    BlobClient mockClient = mock(BlobClient.class);

    InputStream textStream = new ByteArrayInputStream("Hello there.".getBytes());
    BlobInputStream mockStream = mock(BlobInputStream.class);
    when(mockStream.readAllBytes()).thenReturn(textStream.readAllBytes());

    when(mockClient.openInputStream()).thenReturn(mockStream);
    when(mockContainerClient.getBlobClient("hello.txt")).thenReturn(mockClient);
    when(mockServiceClient.getBlobContainerClient("folder")).thenReturn(mockContainerClient);

    storageClient.setServiceClientForTesting(mockServiceClient);
    storageClient.initializeForTesting();
    InputStream result = storageClient.getFileContentStream(testURI);

    assertEquals("Hello there.", new String(result.readAllBytes()));
  }

  @Test
  public void testLastModifiedCutoff() throws Exception {
    Config cloudOptions = ConfigFactory.parseMap(Map.of("connectionString", "connectionString"));
    TestMessenger messenger = new TestMessenger();
    Config connectorConfig = ConfigFactory.parseMap(Map.of(
        // only files modified in the last 2 hours
        "filterOptions", Map.of("lastModifiedCutoff", "2h")
    ));
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    AzureStorageClient azureStorageClient = new AzureStorageClient(cloudOptions);
    TraversalParams params = new TraversalParams(connectorConfig, URI.create("https://storagename.blob.core.windows.net/folder/"), "");

    BlobContainerClient mockClient = mock(BlobContainerClient.class, RETURNS_DEEP_STUBS);
    PagedIterable<BlobItem> pagedIterable = mock(PagedIterable.class);
    when(pagedIterable.stream()).thenReturn(getBlobItemStreamWithCutoff());
    when(mockClient.listBlobs(any(), any())).thenReturn(pagedIterable);

    BlobServiceClient mockServiceClient = mock(BlobServiceClient.class);
    when(mockServiceClient.getBlobContainerClient(any())).thenReturn(mockClient);
    azureStorageClient.setServiceClientForTesting(mockServiceClient);

    azureStorageClient.initializeForTesting();
    azureStorageClient.traverse(publisher, params);
    // only blobs with instant.now() as modified time are returned, there are three of them
    assertEquals(3, publisher.numPublished());
  }

  private Stream<BlobItem> getCompressedAndArchivedBlobStream() {
    BlobItem blobItem1 = new BlobItem();
    blobItem1.setName("jsonlCsvAndFolderWithFooTxt.tar");
    blobItem1.setProperties(new BlobItemProperties()
        .setCreationTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(1L), ZoneId.of("UTC")))
        .setLastModified(OffsetDateTime.ofInstant(Instant.ofEpochMilli(1L), ZoneId.of("UTC")))
        .setContentLength(1L));

    BlobItem blobItem2 = new BlobItem();
    blobItem2.setName("textFiles.tar");
    blobItem2.setProperties(new BlobItemProperties()
        .setCreationTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(2), ZoneId.of("UTC")))
        .setLastModified(OffsetDateTime.ofInstant(Instant.ofEpochMilli(2), ZoneId.of("UTC")))
        .setContentLength(2L));

    BlobItem blobItem3 = new BlobItem();
    blobItem3.setName("jsonlCsvAndFolderWithFooTxt.tar.gz");
    blobItem3.setProperties(new BlobItemProperties()
        .setCreationTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(3), ZoneId.of("UTC")))
        .setLastModified(OffsetDateTime.ofInstant(Instant.ofEpochMilli(3), ZoneId.of("UTC")))
        .setContentLength(3L));

    BlobItem blobItem4 = new BlobItem();
    blobItem4.setName("zipped.csv.zip");
    blobItem4.setProperties(new BlobItemProperties()
        .setCreationTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(4), ZoneId.of("UTC")))
        .setLastModified(OffsetDateTime.ofInstant(Instant.ofEpochMilli(4), ZoneId.of("UTC")))
        .setContentLength(4L));

    BlobItem blobItem5 = new BlobItem();
    blobItem5.setName("zippedFolder.zip");
    blobItem5.setProperties(new BlobItemProperties()
        .setCreationTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(5), ZoneId.of("UTC")))
        .setLastModified(OffsetDateTime.ofInstant(Instant.ofEpochMilli(5), ZoneId.of("UTC")))
        .setContentLength(5L));

    BlobItem blobItem6 = new BlobItem();
    blobItem6.setName("hello.zip");
    blobItem6.setProperties(new BlobItemProperties()
        .setCreationTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(6), ZoneId.of("UTC")))
        .setLastModified(OffsetDateTime.ofInstant(Instant.ofEpochMilli(6), ZoneId.of("UTC")))
        .setContentLength(6L));

    return Stream.of(blobItem1, blobItem2, blobItem3, blobItem4, blobItem5, blobItem6);
  }

  private Stream<BlobItem> getBlobItemStream() {
    BlobItem blobItem1 = new BlobItem();
    blobItem1.setName("blob1");
    blobItem1.setProperties(new BlobItemProperties()
        .setCreationTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(1), ZoneId.of("UTC")))
        .setLastModified(OffsetDateTime.ofInstant(Instant.ofEpochMilli(1), ZoneId.of("UTC")))
        .setContentLength(1L));

    BlobItem blobItem2 = new BlobItem();
    blobItem2.setName("blob2");
    blobItem2.setProperties(new BlobItemProperties()
        .setCreationTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(2), ZoneId.of("UTC")))
        .setLastModified(OffsetDateTime.ofInstant(Instant.ofEpochMilli(2), ZoneId.of("UTC")))
        .setContentLength(2L));

    BlobItem blobItem3 = new BlobItem();
    blobItem3.setName("blob3");
    blobItem3.setProperties(new BlobItemProperties()
        .setCreationTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(3), ZoneId.of("UTC")))
        .setLastModified(OffsetDateTime.ofInstant(Instant.ofEpochMilli(3), ZoneId.of("UTC")))
        .setContentLength(3L));

    BlobItem blobItem4 = new BlobItem();
    blobItem4.setName("blob4");
    blobItem4.setProperties(new BlobItemProperties()
        .setCreationTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(4), ZoneId.of("UTC")))
        .setLastModified(OffsetDateTime.ofInstant(Instant.ofEpochMilli(4), ZoneId.of("UTC")))
        .setContentLength(4L));

    return Stream.of(blobItem1, blobItem2, blobItem3, blobItem4);
  }

  private Stream<BlobItem> getBlobItemStreamWithDirectory() {
    BlobItem blobItem1 = new BlobItem();
    blobItem1.setName("blob1");
    blobItem1.setProperties(new BlobItemProperties()
        .setCreationTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(1), ZoneId.of("UTC")))
        .setLastModified(OffsetDateTime.ofInstant(Instant.ofEpochMilli(1), ZoneId.of("UTC")))
        .setContentLength(1L));

    BlobItem blobItem2 = new BlobItem();
    blobItem2.setName("blob2");
    // setting blobItem2 to be directory
    blobItem2.setIsPrefix(true);
    blobItem2.setProperties(new BlobItemProperties()
        .setCreationTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(2), ZoneId.of("UTC")))
        .setLastModified(OffsetDateTime.ofInstant(Instant.ofEpochMilli(2), ZoneId.of("UTC")))
        .setContentLength(2L));

    BlobItem blobItem3 = new BlobItem();
    blobItem3.setName("blob3");
    blobItem3.setProperties(new BlobItemProperties()
        .setCreationTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(3), ZoneId.of("UTC")))
        .setLastModified(OffsetDateTime.ofInstant(Instant.ofEpochMilli(3), ZoneId.of("UTC")))
        .setContentLength(3L));

    BlobItem blobItem4 = new BlobItem();
    blobItem4.setName("blob4");
    blobItem4.setProperties(new BlobItemProperties()
        .setCreationTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(4), ZoneId.of("UTC")))
        .setLastModified(OffsetDateTime.ofInstant(Instant.ofEpochMilli(4), ZoneId.of("UTC")))
        .setContentLength(4L));

    return Stream.of(blobItem1, blobItem2, blobItem3, blobItem4);
  }

  private Stream<BlobItem> getBlobItemStreamWithCutoff() {
    BlobItem blobItem1 = new BlobItem();
    blobItem1.setName("blob1");
    blobItem1.setProperties(new BlobItemProperties()
        .setCreationTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(1L), ZoneId.of("UTC")))
        .setLastModified(OffsetDateTime.ofInstant(Instant.ofEpochMilli(1L), ZoneId.of("UTC")))
        .setContentLength(1L));

    BlobItem blobItem2 = new BlobItem();
    blobItem2.setName("blob2");
    blobItem2.setProperties(new BlobItemProperties()
        .setCreationTime(OffsetDateTime.ofInstant(Instant.now(), ZoneId.of("UTC")))
        .setLastModified(OffsetDateTime.ofInstant(Instant.now(), ZoneId.of("UTC")))
        .setContentLength(2L));

    BlobItem blobItem3 = new BlobItem();
    blobItem3.setName("blob3");
    blobItem3.setProperties(new BlobItemProperties()
        .setCreationTime(OffsetDateTime.ofInstant(Instant.now(), ZoneId.of("UTC")))
        .setLastModified(OffsetDateTime.ofInstant(Instant.now(), ZoneId.of("UTC")))
        .setContentLength(3L));

    BlobItem blobItem4 = new BlobItem();
    blobItem4.setName("blob4");
    blobItem4.setProperties(new BlobItemProperties()
        .setCreationTime(OffsetDateTime.ofInstant(Instant.now(), ZoneId.of("UTC")))
        .setLastModified(OffsetDateTime.ofInstant(Instant.now(), ZoneId.of("UTC")))
        .setContentLength(4L));

    return Stream.of(blobItem1, blobItem2, blobItem3, blobItem4);
  }

  private Stream<BlobItem> getJsonBlobItemStream() {
    BlobItem blobItem1 = new BlobItem();
    blobItem1.setName("blob1.json");
    blobItem1.setProperties(new BlobItemProperties()
        .setCreationTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(1), ZoneId.of("UTC")))
        .setLastModified(OffsetDateTime.ofInstant(Instant.ofEpochMilli(1), ZoneId.of("UTC")))
        .setContentLength(1L));

    BlobItem blobItem2 = new BlobItem();
    blobItem2.setName("blob2.json");
    blobItem2.setProperties(new BlobItemProperties()
        .setCreationTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(3), ZoneId.of("UTC")))
        .setLastModified(OffsetDateTime.ofInstant(Instant.ofEpochMilli(3), ZoneId.of("UTC")))
        .setContentLength(3L));

    BlobItem blobItem3 = new BlobItem();
    blobItem3.setName("blob3");
    blobItem3.setProperties(new BlobItemProperties()
        .setCreationTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(3), ZoneId.of("UTC")))
        .setLastModified(OffsetDateTime.ofInstant(Instant.ofEpochMilli(3), ZoneId.of("UTC")))
        .setContentLength(3L));

    return Stream.of(blobItem1, blobItem2, blobItem3);
  }

  private List<BlobInputStream> buildMockStreamList(String folderPath) throws Exception {
    List<String> fileNames = List.of("jsonlCsvAndFolderWithFooTxt.tar", "textFiles.tar", "jsonlCsvAndFolderWithFooTxt.tar.gz",
        "zipped.csv.zip", "zippedFolder.zip", "hello.zip");
    List<BlobInputStream> results = new ArrayList<>();

    for (String fileName : fileNames) {
      File currentFile = new File(folderPath + "/" + fileName);
      BufferedInputStream fileStream = new BufferedInputStream(new FileInputStream(currentFile));
      BlobInputStream mockedStream = mock(BlobInputStream.class);
      when(mockedStream.read(any(byte[].class), anyInt(), anyInt())).thenAnswer(invocation -> fileStream.read(invocation.getArgument(0), invocation.getArgument(1), invocation.getArgument(2)));
      results.add(mockedStream);
    }

    return results;
  }
}
