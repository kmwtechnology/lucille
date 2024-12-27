package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.ARCHIVED_FILE_SEPARATOR;
import static com.kmwllc.lucille.connector.FileConnector.CONTENT;
import static com.kmwllc.lucille.connector.FileConnector.FILE_PATH;
import static com.kmwllc.lucille.connector.FileConnector.GET_FILE_CONTENT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import com.kmwllc.lucille.core.fileHandler.CSVFileHandler;
import com.kmwllc.lucille.core.fileHandler.FileHandler;
import com.kmwllc.lucille.core.fileHandler.JsonFileHandler;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class AzureStorageClientTest {

  @Test
  public void testInit() throws Exception{
    Map<String, Object> cloudOptions = Map.of("connectionString", "connectionString");

    AzureStorageClient azureStorageClient = new AzureStorageClient(
        new URI("https://storagename.blob.core.windows.net/testblob"), null, null, null,
        cloudOptions, ConfigFactory.parseMap(Map.of("csv", Map.of(), "json", Map.of())));

    try(MockedConstruction<BlobContainerClientBuilder> builder = Mockito.mockConstruction(BlobContainerClientBuilder.class,(mock,context)-> {
      when(mock.connectionString(anyString())).thenReturn(mock);
      when(mock.containerName(anyString())).thenReturn(mock);
      when(mock.buildClient()).thenReturn(mock(BlobContainerClient.class));
    })) {
      azureStorageClient.init();
      // verify that the file handlers are initialized, 3 in this case due to adding json handler to both json and jsonl keys
      assertEquals(3, azureStorageClient.fileHandlers.size());
      assertInstanceOf(JsonFileHandler.class, azureStorageClient.fileHandlers.get("json"));
      assertInstanceOf(JsonFileHandler.class, azureStorageClient.fileHandlers.get("jsonl"));
      assertEquals(azureStorageClient.fileHandlers.get("json"), azureStorageClient.fileHandlers.get("json"));
      assertInstanceOf(CSVFileHandler.class, azureStorageClient.fileHandlers.get("csv"));

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

    azureStorageClient.shutdown();
  }

  @Test
  public void testShutdown() throws Exception{
    Map<String, Object> cloudOptions = Map.of("connectionString", "connectionString");
    AzureStorageClient azureStorageClient = new AzureStorageClient(
        new URI("https://storagename.blob.core.windows.net/testblob"), null, null, null,
        cloudOptions, ConfigFactory.parseMap(Map.of("csv", Map.of(), "json", Map.of())));
    try(MockedConstruction<BlobContainerClientBuilder> builder = Mockito.mockConstruction(BlobContainerClientBuilder.class,(mock,context)-> {
      when(mock.connectionString(anyString())).thenReturn(mock);
      when(mock.containerName(anyString())).thenReturn(mock);
      when(mock.buildClient()).thenReturn(mock(BlobContainerClient.class));
    })) {
      azureStorageClient.init();
      assertEquals(3, azureStorageClient.fileHandlers.size());
      // test that showdown clears all file handlers
      azureStorageClient.shutdown();
      assertEquals(0, azureStorageClient.fileHandlers.size());
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
    assertNull(documents.get(0).getBytes(CONTENT));
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
    assertEquals("https://storagename.blob.core.windows.net/folder/blob1", doc1.getString(FILE_PATH));
    assertEquals("1", doc1.getString("file_size_bytes"));
    assertArrayEquals(new byte[]{1, 2, 3, 4}, doc1.getBytes(CONTENT));
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
    Map<String, Object> cloudOptions = Map.of("connectionString", "connectionString");
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.parseMap(Map.of());
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    AzureStorageClient azureStorageClient = new AzureStorageClient(new URI("https://storagename.blob.core.windows.net/folder/"), "prefix-",
        List.of(), List.of(), cloudOptions, ConfigFactory.parseMap(
            Map.of(
                "csv", Map.of(),
                "json", Map.of(),
                "handleArchivedFiles", true,
                "handleCompressedFiles", true
            )
    ));

    Map<String, byte[]> fileContents = readAllFilesAsBytesWithMap("src/test/resources/StorageClientTest/testCompressedAndArchived");

    BlobContainerClient mockClient = mock(BlobContainerClient.class, RETURNS_DEEP_STUBS);
    PagedIterable<BlobItem> pagedIterable = mock(PagedIterable.class);
    when(pagedIterable.stream()).thenReturn(getCompressedAndArchivedBlobStream());
    when(mockClient.listBlobs(any(), any())).thenReturn(pagedIterable);
    azureStorageClient.setContainerClientForTesting(mockClient);
    when(mockClient.getBlobClient(anyString()).downloadContent().toBytes())
        .thenReturn(fileContents.get("jsonlCsvAndFolderWithFooTxt.tar")) // blob1
        .thenReturn(fileContents.get("textFiles.tar")) // blob2
        .thenReturn(fileContents.get("jsonlCsvAndFolderWithFooTxt.tar.gz")) // blob3
        .thenReturn(fileContents.get("zipped.csv.zip"))
        .thenReturn(fileContents.get("zippedFolder.zip"))
        .thenReturn(fileContents.get("hello.zip"));

    azureStorageClient.initializeFileHandlers();
    azureStorageClient.traverse(publisher);

    List<Document> docs = messenger.getDocsSentForProcessing();

    assertEquals(19, docs.size());

    // check documents published from jsonlCsvAndFolderWithFooTxt.tar
    Document doc1 = docs.get(0);
    assertEquals("default.csv-1", doc1.getId());
    assertEquals("https://storagename.blob.core.windows.net/folder/jsonlCsvAndFolderWithFooTxt.tar"
        + ARCHIVED_FILE_SEPARATOR + "default.csv", doc1.getString("source"));
    Document doc2 = docs.get(1);
    assertEquals("default.csv-2", doc2.getId());
    assertEquals("https://storagename.blob.core.windows.net/folder/jsonlCsvAndFolderWithFooTxt.tar"
        + ARCHIVED_FILE_SEPARATOR + "default.csv", doc2.getString("source"));
    Document doc3 = docs.get(2);
    assertEquals("default.csv-3", doc3.getId());
    assertEquals("https://storagename.blob.core.windows.net/folder/jsonlCsvAndFolderWithFooTxt.tar"
        + ARCHIVED_FILE_SEPARATOR + "default.csv", doc3.getString("source"));
    Document doc4 = docs.get(3);
    assertEquals("2", doc4.getId());
    assertEquals("Gorgeous Woman Mug", doc4.getString("name"));
    Document doc5 = docs.get(4);
    assertEquals("3", doc5.getId());
    assertEquals("Awesome City Mug", doc5.getString("name"));
    Document doc6 = docs.get(5);
    assertEquals("https://storagename.blob.core.windows.net/folder/jsonlCsvAndFolderWithFooTxt.tar"
        + ARCHIVED_FILE_SEPARATOR + "FolderWithFooTxt/foo.txt", doc6.getString(FILE_PATH));

    // check documents published from textFiles.tar
    Document doc7 = docs.get(6);
    assertEquals("https://storagename.blob.core.windows.net/folder/textFiles.tar" + ARCHIVED_FILE_SEPARATOR + "helloWorld.txt", doc7.getString(FILE_PATH));
    Document doc8 = docs.get(7);
    assertEquals("https://storagename.blob.core.windows.net/folder/textFiles.tar" + ARCHIVED_FILE_SEPARATOR + "goodbye.txt", doc8.getString(FILE_PATH));

    // check documents published from jsonlCsvAndFolderWithFooTxt.tar.gz
    Document doc9 = docs.get(8);
    assertEquals("default.csv-1", doc9.getId());
    assertEquals("https://storagename.blob.core.windows.net/folder/jsonlCsvAndFolderWithFooTxt.tar.gz"
        + ARCHIVED_FILE_SEPARATOR + "default.csv", doc9.getString("source"));
    Document doc10 = docs.get(9);
    assertEquals("default.csv-2", doc10.getId());
    assertEquals("https://storagename.blob.core.windows.net/folder/jsonlCsvAndFolderWithFooTxt.tar.gz"
        + ARCHIVED_FILE_SEPARATOR + "default.csv", doc10.getString("source"));
    Document doc11 = docs.get(10);
    assertEquals("default.csv-3", doc11.getId());
    assertEquals("https://storagename.blob.core.windows.net/folder/jsonlCsvAndFolderWithFooTxt.tar.gz"
        + ARCHIVED_FILE_SEPARATOR + "default.csv", doc11.getString("source"));
    Document doc12 = docs.get(11);
    assertEquals("2", doc12.getId());
    assertEquals("Gorgeous Woman Mug", doc12.getString("name"));
    Document doc13 = docs.get(12);
    assertEquals("3", doc13.getId());
    assertEquals("Awesome City Mug", doc13.getString("name"));
    Document doc14 = docs.get(13);
    assertEquals("https://storagename.blob.core.windows.net/folder/jsonlCsvAndFolderWithFooTxt.tar.gz"
        + ARCHIVED_FILE_SEPARATOR + "FolderWithFooTxt/foo.txt", doc14.getString(FILE_PATH));
    // check documents published from zipped.csv.zip
    Document doc15 = docs.get(14);
    assertEquals("zipped.csv-1", doc15.getId());
    assertEquals("https://storagename.blob.core.windows.net/folder/zipped.csv.zip"
        + ARCHIVED_FILE_SEPARATOR + "zipped.csv", doc15.getString("source"));
    Document doc16 = docs.get(15);
    assertEquals("zipped.csv-2", doc16.getId());
    assertEquals("https://storagename.blob.core.windows.net/folder/zipped.csv.zip"
        + ARCHIVED_FILE_SEPARATOR + "zipped.csv", doc16.getString("source"));
    Document doc17 = docs.get(16);
    assertEquals("zipped.csv-3", doc17.getId());
    assertEquals("https://storagename.blob.core.windows.net/folder/zipped.csv.zip"
        + ARCHIVED_FILE_SEPARATOR + "zipped.csv", doc17.getString("source"));
    // check documents published from zippedFolder.zip
    Document doc18 = docs.get(17);
    assertEquals("https://storagename.blob.core.windows.net/folder/zippedFolder.zip"
        + ARCHIVED_FILE_SEPARATOR + "zippedFolder/foo.txt", doc18.getString(FILE_PATH));
    // check documents published from hello.zip
    Document doc19 = docs.get(18);
    assertEquals("https://storagename.blob.core.windows.net/folder/hello.zip"
        + ARCHIVED_FILE_SEPARATOR + "hello", doc19.getString(FILE_PATH));

    azureStorageClient.shutdown();
  }

  @Test
  public void testErrorMovingFiles() throws Exception{
    Map<String, Object> cloudOptions = Map.of("connectionString", "connectionString");
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.parseMap(Map.of());
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    AzureStorageClient azureStorageClient = new AzureStorageClient(new URI("https://storagename.blob.core.windows.net/folder/"), "prefix-",
        List.of(), List.of(), cloudOptions, ConfigFactory.parseMap(
            Map.of(
                "moveToAfterProcessing", "https://storagename.blob.core.windows.net/folder/processed",
                "moveToErrorFolder", "https://storagename.blob.core.windows.net/folder/error"
            )
    ));

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

    assertThrows(UnsupportedOperationException.class, () -> azureStorageClient.traverse(publisher));

    azureStorageClient.shutdown();
  }

  private Stream<BlobItem> getCompressedAndArchivedBlobStream() {
    BlobItem blobItem1 = new BlobItem();
    blobItem1.setName("jsonlCsvAndFolderWithFooTxt.tar");
    blobItem1.setProperties(new BlobItemProperties()
        .setCreationTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(1), ZoneId.of("UTC")))
        .setLastModified(OffsetDateTime.ofInstant(Instant.ofEpochMilli(1), ZoneId.of("UTC")))
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

  private static Map<String, byte[]> readAllFilesAsBytesWithMap(String folderPath) throws Exception {
    Map<String, byte[]> fileBytesMap = new LinkedHashMap<>();

    File folder = new File(folderPath);

    if (folder.exists() && folder.isDirectory()) {
      File[] files = folder.listFiles();
      if (files != null) {
        for (File file : files) {
          if (file.isFile()) {
            byte[] fileBytes = Files.readAllBytes(file.toPath());
            fileBytesMap.put(file.getName(), fileBytes);
          }
        }
      }
    } else {
      throw new IllegalArgumentException("Invalid folder path: " + folderPath);
    }

    return fileBytesMap;
  }
}
