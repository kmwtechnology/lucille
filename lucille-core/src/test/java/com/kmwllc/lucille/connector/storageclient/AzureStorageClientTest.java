package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.GET_FILE_CONTENT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.core.fileHandler.FileHandler;
import com.kmwllc.lucille.core.fileHandler.JsonFileHandler;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.net.URI;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class AzureStorageClientTest {

  @Test
  public void testInit() throws Exception{
    Map<String, Object> cloudOptions = Map.of("connectionString", "connectionString");

    AzureStorageClient azureStorageClient = new AzureStorageClient(new URI("https://storagename.blob.core.windows.net/testblob"), null, null, null, cloudOptions, ConfigFactory.empty());

    try(MockedConstruction<BlobContainerClientBuilder> builder = Mockito.mockConstruction(BlobContainerClientBuilder.class,(mock,context)-> {
      when(mock.connectionString(anyString())).thenReturn(mock);
      when(mock.containerName(anyString())).thenReturn(mock);
      when(mock.buildClient()).thenReturn(mock(BlobContainerClient.class));
    })) {
      azureStorageClient.init();
      verify(builder.constructed().get(0), times(1)).connectionString("connectionString");
    }

    cloudOptions = new HashMap<>();
    cloudOptions.put("accountName", "accountName");
    cloudOptions.put("accountKey", "accountKey");

    azureStorageClient = new AzureStorageClient(new URI("https://storagename.blob.core.windows.net/testblob"),
        null, null, null, cloudOptions, ConfigFactory.empty());

    try(MockedConstruction<BlobContainerClientBuilder> builder = Mockito.mockConstruction(BlobContainerClientBuilder.class,(mock,context)-> {

      when(mock.credential((StorageSharedKeyCredential) any())).thenReturn(mock);
      when(mock.containerName(anyString())).thenReturn(mock);
      when(mock.buildClient()).thenReturn(mock(BlobContainerClient.class));
    })) {
      azureStorageClient.init();
      verify(builder.constructed().get(0), times(1)).credential(any(StorageSharedKeyCredential.class));
    }
  }

  @Test
  public void testPublishValidFiles() throws Exception {
    Map<String, Object> cloudOptions = Map.of("connectionString", "connectionString");
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.parseMap(Map.of());
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    AzureStorageClient azureStorageClient = new AzureStorageClient(new URI("https://storagename.blob.core.windows.net/folder/"), "prefix-",
        List.of(), List.of(), cloudOptions, ConfigFactory.empty());

    BlobContainerClient mockClient = mock(BlobContainerClient.class, RETURNS_DEEP_STUBS);
    PagedIterable<BlobItem> pagedIterable = mock(PagedIterable.class);
    when(pagedIterable.stream()).thenReturn(getBlobItemStream());
    when(mockClient.listBlobs(any(), any())).thenReturn(pagedIterable);
    azureStorageClient.setContainerClientForTesting(mockClient);
    when(mockClient.getBlobClient(anyString()).downloadContent().toBytes())
        .thenReturn(new byte[]{1, 2, 3, 4}) // blob1
        .thenReturn(new byte[]{5, 6, 7, 8}) // blob2
        .thenReturn(new byte[]{9, 10, 11, 12}) // blob3
        .thenReturn(new byte[]{13, 14, 15, 16}); // blob4

    azureStorageClient.traverse(publisher);

    List<Document> documents = messenger.getDocsSentForProcessing();

    // all documents processed
    assertEquals(4, documents.size());
    Document doc1 = documents.get(0);
    assertTrue(doc1.getId().startsWith("prefix-"));
    assertEquals("https://storagename.blob.core.windows.net/folder/blob1", doc1.getString("file_path"));
    assertEquals("1", doc1.getString("file_size_bytes"));
    assertArrayEquals(new byte[]{1, 2, 3, 4}, doc1.getBytes("file_content"));

    Document doc2 = documents.get(1);
    assertTrue(doc2.getId().startsWith("prefix-"));
    assertEquals("https://storagename.blob.core.windows.net/folder/blob2", doc2.getString("file_path"));
    assertEquals("2", doc2.getString("file_size_bytes"));
    assertArrayEquals(new byte[]{5, 6, 7, 8}, doc2.getBytes("file_content"));

    Document doc3 = documents.get(2);
    assertTrue(doc3.getId().startsWith("prefix-"));
    assertEquals("https://storagename.blob.core.windows.net/folder/blob3", doc3.getString("file_path"));
    assertEquals("3", doc3.getString("file_size_bytes"));
    assertArrayEquals(new byte[]{9, 10, 11, 12}, doc3.getBytes("file_content"));

    Document doc4 = documents.get(3);
    assertTrue(doc4.getId().startsWith("prefix-"));
    assertEquals("https://storagename.blob.core.windows.net/folder/blob4", doc4.getString("file_path"));
    assertEquals("4", doc4.getString("file_size_bytes"));
    assertArrayEquals(new byte[]{13, 14, 15, 16}, doc4.getBytes("file_content"));
  }

  @Test
  public void testSkipFileContent() throws Exception {
    Map<String, Object> cloudOptions = Map.of("connectionString", "connectionString");
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.parseMap(Map.of());
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    AzureStorageClient azureStorageClient = new AzureStorageClient(new URI("https://storagename.blob.core.windows.net/folder/"), "prefix-",
        List.of(), List.of(), cloudOptions, ConfigFactory.parseMap(Map.of(GET_FILE_CONTENT, false)));

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
    azureStorageClient.setContainerClientForTesting(mockClient);
    when(mockClient.getBlobClient(anyString()).downloadContent().toBytes())
        .thenReturn(new byte[]{1, 2, 3, 4});

    azureStorageClient.traverse(publisher);

    List<Document> documents = messenger.getDocsSentForProcessing();

    // only 1 blob
    assertEquals(1, documents.size());
    // no fileContent
    assertNull(documents.get(0).getBytes("file_content"));
  }

  @Test
  public void testPublishInvalidFiles() throws Exception {
    Map<String, Object> cloudOptions = Map.of("connectionString", "connectionString");
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.parseMap(Map.of());
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    AzureStorageClient azureStorageClient = new AzureStorageClient(new URI("https://storagename.blob.core.windows.net/folder/"), "prefix-",
        List.of(Pattern.compile("blob3"), Pattern.compile("blob4")), List.of(), cloudOptions, ConfigFactory.empty());

    BlobContainerClient mockClient = mock(BlobContainerClient.class, RETURNS_DEEP_STUBS);
    PagedIterable<BlobItem> pagedIterable = mock(PagedIterable.class);
    when(pagedIterable.stream()).thenReturn(getBlobItemStreamWithDirectory());
    when(mockClient.listBlobs(any(), any())).thenReturn(pagedIterable);
    azureStorageClient.setContainerClientForTesting(mockClient);
    when(mockClient.getBlobClient(anyString()).downloadContent().toBytes())
        .thenReturn(new byte[]{1, 2, 3, 4}) // blob1
        .thenReturn(new byte[]{5, 6, 7, 8}) // blob2
        .thenReturn(new byte[]{9, 10, 11, 12}) // blob3
        .thenReturn(new byte[]{13, 14, 15, 16}); // blob4

    azureStorageClient.traverse(publisher);

    List<Document> documents = messenger.getDocsSentForProcessing();

    // only blob1 processed due to blob2 being a directory and blob3 and blob4 being excluded via regex
    assertEquals(1, documents.size());
    Document doc1 = documents.get(0);
    assertTrue(doc1.getId().startsWith("prefix-"));
    assertEquals("https://storagename.blob.core.windows.net/folder/blob1", doc1.getString("file_path"));
    assertEquals("1", doc1.getString("file_size_bytes"));
    assertArrayEquals(new byte[]{1, 2, 3, 4}, doc1.getBytes("file_content"));
  }

  @Test
  public void testPublishUsingFileHandler() throws Exception {
    Map<String, Object> cloudOptions = Map.of("connectionString", "connectionString");
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.parseMap(Map.of());
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    // azure storage client that handles json files
    AzureStorageClient azureStorageClient = new AzureStorageClient(new URI("https://storagename.blob.core.windows.net/folder/"), "prefix-",
        List.of(), List.of(), cloudOptions, ConfigFactory.parseMap(Map.of("json", Map.of(), GET_FILE_CONTENT, false)));


    BlobContainerClient mockClient = mock(BlobContainerClient.class, RETURNS_DEEP_STUBS);
    PagedIterable<BlobItem> pagedIterable = mock(PagedIterable.class);
    when(pagedIterable.stream()).thenReturn(getJsonBlobItemStream());
    when(mockClient.listBlobs(any(), any())).thenReturn(pagedIterable);
    azureStorageClient.setContainerClientForTesting(mockClient);
    try (MockedStatic<FileHandler> mockFileHandler = mockStatic(FileHandler.class)) {
      FileHandler jsonFileHandler = mock(JsonFileHandler.class);
      mockFileHandler.when(() -> FileHandler.create(any(), any()))
          .thenReturn(jsonFileHandler);
      mockFileHandler.when(() -> FileHandler.supportAndContainFileType(any(), any()))
          .thenReturn(true).thenReturn(true).thenReturn(false);

      azureStorageClient.initializeFileHandlers();
      azureStorageClient.traverse(publisher);
      // verify that the processFileAndPublish is only called for the json files
      verify(jsonFileHandler, times(2)).processFileAndPublish(any(), any(), any());
    }

    // shutdown client and handlers
    azureStorageClient.shutdown();
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
}
