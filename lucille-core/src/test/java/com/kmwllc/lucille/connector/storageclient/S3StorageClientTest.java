package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.ARCHIVE_FILE_SEPARATOR;
import static com.kmwllc.lucille.connector.FileConnector.CONTENT;
import static com.kmwllc.lucille.connector.FileConnector.FILE_PATH;
import static com.kmwllc.lucille.connector.FileConnector.GET_FILE_CONTENT;
import static com.kmwllc.lucille.connector.FileConnector.S3_ACCESS_KEY_ID;
import static com.kmwllc.lucille.connector.FileConnector.S3_REGION;
import static com.kmwllc.lucille.connector.FileConnector.S3_SECRET_ACCESS_KEY;
import static com.kmwllc.lucille.connector.FileConnector.SIZE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.core.fileHandler.CSVFileHandler;
import com.kmwllc.lucille.core.fileHandler.FileHandler;
import com.kmwllc.lucille.core.fileHandler.JsonFileHandler;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

public class S3StorageClientTest {

  S3Object obj1;
  S3Object obj2;
  S3Object obj3;
  S3Object obj4;

  @Test
  public void testInitFileHandlers() throws Exception{
    Map<String, Object> cloudOptions = Map.of(S3_REGION, "us-east-1", S3_ACCESS_KEY_ID, "accessKey",
        S3_SECRET_ACCESS_KEY, "secretKey");
    S3StorageClient s3StorageClient = new S3StorageClient(new URI("s3://bucket/"), null,
        null, null, cloudOptions, ConfigFactory.parseMap(Map.of("json", Map.of(), "csv", Map.of())));

    s3StorageClient.init();

    // check that the file handlers are initialized, note that it is 3 because json fileHandler will be added to jsonl and json
    assertEquals(3, s3StorageClient.fileHandlers.size());
    assertInstanceOf(JsonFileHandler.class, s3StorageClient.fileHandlers.get("jsonl"));
    assertInstanceOf(JsonFileHandler.class, s3StorageClient.fileHandlers.get("json"));
    assertEquals(s3StorageClient.fileHandlers.get("json"), s3StorageClient.fileHandlers.get("jsonl"));

    assertInstanceOf(CSVFileHandler.class, s3StorageClient.fileHandlers.get("csv"));

    s3StorageClient.shutdown();
  }

  @Test
  public void testShutdown() throws Exception {
    Map<String, Object> cloudOptions = Map.of(S3_REGION, "us-east-1", S3_ACCESS_KEY_ID, "accessKey",
        S3_SECRET_ACCESS_KEY, "secretKey");
    S3StorageClient s3StorageClient = new S3StorageClient(new URI("s3://bucket/"), null,
        null, null, cloudOptions, ConfigFactory.parseMap(Map.of("csv", Map.of())));

    s3StorageClient.init();
    assertEquals(1, s3StorageClient.fileHandlers.size());
    S3Client mockClient = mock(S3Client.class);
    s3StorageClient.setS3ClientForTesting(mockClient);
    s3StorageClient.shutdown();

    // verify that the s3 client is closed
    verify(mockClient, times(1)).close();
    // check that the file handlers are cleared
    assertEquals(0, s3StorageClient.fileHandlers.size());
  }

  @Test
  public void testPublishValidFiles() throws Exception {
    Map<String, Object> cloudOptions = Map.of(S3_REGION, "us-east-1", S3_ACCESS_KEY_ID, "accessKey",
        S3_SECRET_ACCESS_KEY, "secretKey");
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.parseMap(Map.of());
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    S3StorageClient s3StorageClient = new S3StorageClient(new URI("s3://bucket/"), "prefix-",
        List.of(), List.of(), cloudOptions, ConfigFactory.empty());

    S3Client mockClient = mock(S3Client.class, RETURNS_DEEP_STUBS);
    s3StorageClient.setS3ClientForTesting(mockClient);
    ListObjectsV2Iterable response = mock(ListObjectsV2Iterable.class);
    ListObjectsV2Response responseWithinStream = mock(ListObjectsV2Response.class);
    when(responseWithinStream.contents()).thenReturn(getMockedS3Objects());
    when(response.stream()).thenReturn(Stream.of(responseWithinStream));

    when(mockClient.listObjectsV2Paginator((ListObjectsV2Request) any())).thenReturn(response);

    when(mockClient.getObjectAsBytes((GetObjectRequest) any()))
        .thenReturn(ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), new byte[]{1, 2, 3, 4}))
        .thenReturn(ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), new byte[]{5, 6, 7, 8}))
        .thenReturn(ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), new byte[]{9, 10, 11, 12}))
        .thenReturn(ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), new byte[]{13, 14, 15, 16}));

    s3StorageClient.traverse(publisher);

    List<Document> documents = messenger.getDocsSentForProcessing();

    // all documents processed
    assertEquals(4, documents.size());
    Document doc1 = documents.get(0);
    assertTrue(doc1.getId().startsWith("prefix-"));
    assertEquals("s3://bucket/obj1", doc1.getString(FILE_PATH));
    assertEquals("1", doc1.getString(SIZE));
    assertArrayEquals(new byte[]{1, 2, 3, 4}, doc1.getBytes(CONTENT));

    Document doc2 = documents.get(1);
    assertTrue(doc2.getId().startsWith("prefix-"));
    assertEquals("s3://bucket/obj2", doc2.getString(FILE_PATH));
    assertEquals("2", doc2.getString(SIZE));
    assertArrayEquals(new byte[]{5, 6, 7, 8}, doc2.getBytes(CONTENT));

    Document doc3 = documents.get(2);
    assertTrue(doc3.getId().startsWith("prefix-"));
    assertEquals("s3://bucket/obj3", doc3.getString(FILE_PATH));
    assertEquals("3", doc3.getString(SIZE));
    assertArrayEquals(new byte[]{9, 10, 11, 12}, doc3.getBytes(CONTENT));

    Document doc4 = documents.get(3);
    assertTrue(doc4.getId().startsWith("prefix-"));
    assertEquals("s3://bucket/obj4", doc4.getString(FILE_PATH));
    assertEquals("4", doc4.getString(SIZE));
    assertArrayEquals(new byte[]{13, 14, 15, 16}, doc4.getBytes(CONTENT));
  }


  @Test
  public void testExcludes() throws Exception {
    Map<String, Object> cloudOptions = Map.of(S3_REGION, "us-east-1", S3_ACCESS_KEY_ID, "accessKey",
        S3_SECRET_ACCESS_KEY, "secretKey");
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.parseMap(Map.of());
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    S3StorageClient s3StorageClient = new S3StorageClient(new URI("s3://bucket/"), "prefix-",
        List.of(Pattern.compile("obj3"), Pattern.compile("obj4")), List.of() , cloudOptions, ConfigFactory.empty());

    S3Client mockClient = mock(S3Client.class, RETURNS_DEEP_STUBS);
    s3StorageClient.setS3ClientForTesting(mockClient);
    ListObjectsV2Iterable response = mock(ListObjectsV2Iterable.class);
    ListObjectsV2Response responseWithinStream = mock(ListObjectsV2Response.class);
    when(responseWithinStream.contents()).thenReturn(getMockedS3ObjectsWithDirectory());
    when(response.stream()).thenReturn(Stream.of(responseWithinStream));

    when(mockClient.listObjectsV2Paginator((ListObjectsV2Request) any())).thenReturn(response);

    when(mockClient.getObjectAsBytes((GetObjectRequest) any()))
        .thenReturn(ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), new byte[]{1, 2, 3, 4}))
        .thenReturn(ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), new byte[]{5, 6, 7, 8}))
        .thenReturn(ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), new byte[]{9, 10, 11, 12}))
        .thenReturn(ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), new byte[]{13, 14, 15, 16}));

    s3StorageClient.traverse(publisher);

    List<Document> documents = messenger.getDocsSentForProcessing();
    // only obj1 processed, skipping over directory obj2, and obj3 and obj4 because of exclude regex
    assertEquals(1, documents.size());
    Document doc1 = documents.get(0);
    assertTrue(doc1.getId().startsWith("prefix-"));
    assertEquals("s3://bucket/obj1", doc1.getString(FILE_PATH));
    assertEquals("1", doc1.getString(SIZE));
    assertArrayEquals(new byte[]{1, 2, 3, 4}, doc1.getBytes(CONTENT));
  }

  @Test
  public void testSkipFileContent() throws Exception {
    Map<String, Object> cloudOptions = Map.of(S3_REGION, "us-east-1", S3_ACCESS_KEY_ID, "accessKey",
        S3_SECRET_ACCESS_KEY, "secretKey");
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.parseMap(Map.of());
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    S3StorageClient s3StorageClient = new S3StorageClient(new URI("s3://bucket/"), "prefix-",
        List.of(), List.of(), cloudOptions, ConfigFactory.parseMap(Map.of(GET_FILE_CONTENT, false)));

    S3Client mockClient = mock(S3Client.class, RETURNS_DEEP_STUBS);
    s3StorageClient.setS3ClientForTesting(mockClient);
    ListObjectsV2Iterable response = mock(ListObjectsV2Iterable.class);
    ListObjectsV2Response responseWithinStream = mock(ListObjectsV2Response.class);

    obj1 = S3Object.builder().key("obj1").lastModified(Instant.ofEpochMilli(1L)).size(1L).build();
    when(responseWithinStream.contents()).thenReturn(List.of(obj1));
    when(response.stream()).thenReturn(Stream.of(responseWithinStream));

    when(mockClient.listObjectsV2Paginator((ListObjectsV2Request) any())).thenReturn(response);

    when(mockClient.getObjectAsBytes((GetObjectRequest) any()))
        .thenReturn(ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), new byte[]{1, 2, 3, 4}));

    s3StorageClient.traverse(publisher);

    List<Document> documents = messenger.getDocsSentForProcessing();

    // check that only one document published without file_content
    assertEquals(1, documents.size());
    assertNull(documents.get(0).getBytes(CONTENT));
  }

  @Test
  public void testPublishUsingFileHandler() throws Exception {
    Map<String, Object> cloudOptions = Map.of(S3_REGION, "us-east-1", S3_ACCESS_KEY_ID, "accessKey",
        S3_SECRET_ACCESS_KEY, "secretKey");
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.parseMap(Map.of());
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    // storage client with json file handler
    S3StorageClient s3StorageClient = new S3StorageClient(new URI("s3://bucket/"), "prefix-",
        List.of(), List.of(), cloudOptions, ConfigFactory.parseMap(Map.of("json", Map.of())));

    S3Client mockClient = mock(S3Client.class, RETURNS_DEEP_STUBS);
    s3StorageClient.setS3ClientForTesting(mockClient);
    ListObjectsV2Iterable response = mock(ListObjectsV2Iterable.class);
    ListObjectsV2Response responseWithinStream = mock(ListObjectsV2Response.class);
    when(responseWithinStream.contents()).thenReturn(getMockedS3JsonObjects());
    when(response.stream()).thenReturn(Stream.of(responseWithinStream));

    when(mockClient.listObjectsV2Paginator((ListObjectsV2Request) any())).thenReturn(response);

    try (MockedStatic<FileHandler> mockFileHandler = mockStatic(FileHandler.class)) {
      FileHandler jsonFileHandler = mock(JsonFileHandler.class);
      mockFileHandler.when(() -> FileHandler.create(any(), any()))
          .thenReturn(jsonFileHandler);
      mockFileHandler.when(() -> FileHandler.supportAndContainFileType(any(), any()))
          .thenReturn(true).thenReturn(true).thenReturn(false);

      s3StorageClient.initializeFileHandlers();
      s3StorageClient.traverse(publisher);
      // verify that the processFileAndPublish is only called for the json files
      ArgumentCaptor<String> fileNameCaptor = ArgumentCaptor.forClass(String.class);
      verify(jsonFileHandler, times(2)).processFileAndPublish(any(), any(InputStream.class), fileNameCaptor.capture());
      List<String> fileNames = fileNameCaptor.getAllValues();
      assertEquals("s3://bucket/obj1.json", fileNames.get(0));
      assertEquals("s3://bucket/obj2.json", fileNames.get(1));
    }

    s3StorageClient.shutdown();
  }

  @Test
  public void testPublishOnCompressedAndArchived() throws Exception {
    Map<String, Object> cloudOptions = Map.of(S3_REGION, "us-east-1", S3_ACCESS_KEY_ID, "accessKey",
        S3_SECRET_ACCESS_KEY, "secretKey");
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.parseMap(Map.of());
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    // storage client with json file handler
    S3StorageClient s3StorageClient = new S3StorageClient(new URI("s3://bucket/"), "",
        List.of(), List.of(), cloudOptions, ConfigFactory.parseMap(Map.of(
            "json", Map.of(),
        "csv", Map.of(),
        "handleArchivedFiles", true,
        "handleCompressedFiles", true)));

    S3Client mockClient = mock(S3Client.class, RETURNS_DEEP_STUBS);
    s3StorageClient.setS3ClientForTesting(mockClient);
    ListObjectsV2Iterable response = mock(ListObjectsV2Iterable.class);
    ListObjectsV2Response responseWithinStream = mock(ListObjectsV2Response.class);
    when(responseWithinStream.contents()).thenReturn(getMockedS3ObjectsWithCompressionAndArchive());
    when(response.stream()).thenReturn(Stream.of(responseWithinStream));

    when(mockClient.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(response);

    String folderPath = new File("src/test/resources/StorageClientTest/testCompressedAndArchived").getAbsolutePath();
    List<ResponseInputStream<GetObjectResponse>> mockedStreams = buildMockStreamList(folderPath);

    when(mockClient.getObject(any(GetObjectRequest.class)))
        .thenReturn(mockedStreams.get(0))
        .thenReturn(mockedStreams.get(1))
        .thenReturn(mockedStreams.get(2))
        .thenReturn(mockedStreams.get(3))
        .thenReturn(mockedStreams.get(4))
        .thenReturn(mockedStreams.get(5));

    s3StorageClient.initializeFileHandlers();
    s3StorageClient.traverse(publisher);

    List<Document> docs = messenger.getDocsSentForProcessing();

    assertEquals(19, docs.size());

    // check documents published from jsonlCsvAndFolderWithFooTxt.tar
    Document doc1 = docs.get(0);
    assertEquals("default.csv-1", doc1.getId());
    assertEquals("s3://bucket/jsonlCsvAndFolderWithFooTxt.tar" + ARCHIVE_FILE_SEPARATOR + "default.csv", doc1.getString("source"));
    Document doc2 = docs.get(1);
    assertEquals("default.csv-2", doc2.getId());
    assertEquals("s3://bucket/jsonlCsvAndFolderWithFooTxt.tar" + ARCHIVE_FILE_SEPARATOR + "default.csv", doc2.getString("source"));
    Document doc3 = docs.get(2);
    assertEquals("default.csv-3", doc3.getId());
    assertEquals("s3://bucket/jsonlCsvAndFolderWithFooTxt.tar" + ARCHIVE_FILE_SEPARATOR + "default.csv", doc3.getString("source"));
    Document doc4 = docs.get(3);
    assertEquals("2", doc4.getId());
    assertEquals("Gorgeous Woman Mug", doc4.getString("name"));
    Document doc5 = docs.get(4);
    assertEquals("3", doc5.getId());
    assertEquals("Awesome City Mug", doc5.getString("name"));
    Document doc6 = docs.get(5);
    assertEquals("s3://bucket/jsonlCsvAndFolderWithFooTxt.tar" + ARCHIVE_FILE_SEPARATOR + "FolderWithFooTxt/foo.txt", doc6.getString(FILE_PATH));

    // check document published from hello.zip
    Document doc7 = docs.get(6);
    assertEquals("s3://bucket/hello.zip" + ARCHIVE_FILE_SEPARATOR + "hello", doc7.getString(FILE_PATH));
    // check documents published from textFiles.tar
    Document doc8 = docs.get(7);
    assertEquals("s3://bucket/textFiles.tar" + ARCHIVE_FILE_SEPARATOR + "helloWorld.txt", doc8.getString(FILE_PATH));
    Document doc9 = docs.get(8);
    assertEquals("s3://bucket/textFiles.tar" + ARCHIVE_FILE_SEPARATOR + "goodbye.txt", doc9.getString(FILE_PATH));
    // check documents published from jsonlCsvAndFolderWithFooTxt.tar.gz
    Document doc10 = docs.get(9);
    assertEquals("default.csv-1", doc10.getId());
    assertEquals("s3://bucket/jsonlCsvAndFolderWithFooTxt.tar.gz" + ARCHIVE_FILE_SEPARATOR + "default.csv", doc10.getString("source"));
    Document doc11 = docs.get(10);
    assertEquals("default.csv-2", doc11.getId());
    assertEquals("s3://bucket/jsonlCsvAndFolderWithFooTxt.tar.gz" + ARCHIVE_FILE_SEPARATOR + "default.csv", doc11.getString("source"));
    Document doc12 = docs.get(11);
    assertEquals("default.csv-3", doc12.getId());
    assertEquals("s3://bucket/jsonlCsvAndFolderWithFooTxt.tar.gz" + ARCHIVE_FILE_SEPARATOR + "default.csv", doc12.getString("source"));
    Document doc13 = docs.get(12);
    assertEquals("2", doc13.getId());
    assertEquals("Gorgeous Woman Mug", doc13.getString("name"));
    Document doc14 = docs.get(13);
    assertEquals("3", doc14.getId());
    assertEquals("Awesome City Mug", doc14.getString("name"));
    Document doc15 = docs.get(14);
    assertEquals("s3://bucket/jsonlCsvAndFolderWithFooTxt.tar.gz" + ARCHIVE_FILE_SEPARATOR + "FolderWithFooTxt/foo.txt", doc15.getString(FILE_PATH));
    // check document published from zippedFolder.zip
    Document doc16 = docs.get(15);
    assertEquals("s3://bucket/zippedFolder.zip" + ARCHIVE_FILE_SEPARATOR + "zippedFolder/foo.txt", doc16.getString(FILE_PATH));
    // check documents published from zipped.csv.zip
    Document doc17 = docs.get(16);
    assertEquals("zipped.csv-1", doc17.getId());
    assertEquals("s3://bucket/zipped.csv.zip" + ARCHIVE_FILE_SEPARATOR + "zipped.csv", doc17.getString("source"));
    Document doc18 = docs.get(17);
    assertEquals("zipped.csv-2", doc18.getId());
    assertEquals("s3://bucket/zipped.csv.zip" + ARCHIVE_FILE_SEPARATOR + "zipped.csv", doc18.getString("source"));
    Document doc19 = docs.get(18);
    assertEquals("zipped.csv-3", doc19.getId());
    assertEquals("s3://bucket/zipped.csv.zip" + ARCHIVE_FILE_SEPARATOR + "zipped.csv", doc19.getString("source"));

    s3StorageClient.shutdown();
  }

  @Test
  public void testErrorMovingFiles() throws Exception {
    Map<String, Object> cloudOptions = Map.of(S3_REGION, "us-east-1", S3_ACCESS_KEY_ID, "accessKey",
        S3_SECRET_ACCESS_KEY, "secretKey");
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.parseMap(Map.of());
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    // simulate a proper s3StorageClient with valid files
    S3StorageClient s3StorageClient = new S3StorageClient(new URI("s3://bucket/"), "",
        List.of(), List.of(), cloudOptions, ConfigFactory.parseMap(Map.of(
        "moveToAfterProcessing", "s3://bucket/Processed",
        "moveToErrorFolder", "s3://bucket/Error")));
    S3Client mockClient = mock(S3Client.class, RETURNS_DEEP_STUBS);
    s3StorageClient.setS3ClientForTesting(mockClient);
    ListObjectsV2Iterable response = mock(ListObjectsV2Iterable.class);
    ListObjectsV2Response responseWithinStream = mock(ListObjectsV2Response.class);
    when(responseWithinStream.contents()).thenReturn(getMockedS3Objects());
    when(response.stream()).thenReturn(Stream.of(responseWithinStream));
    when(mockClient.listObjectsV2Paginator((ListObjectsV2Request) any())).thenReturn(response);
    when(mockClient.getObjectAsBytes((GetObjectRequest) any()))
        .thenReturn(ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), new byte[]{1, 2, 3, 4}))
        .thenReturn(ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), new byte[]{5, 6, 7, 8}))
        .thenReturn(ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), new byte[]{9, 10, 11, 12}))
        .thenReturn(ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), new byte[]{13, 14, 15, 16}));

    // encounter error when traversing if moveAfterProcessing is set
    assertThrows(UnsupportedOperationException.class, () -> s3StorageClient.traverse(publisher));
    s3StorageClient.shutdown();
  }

  private List<S3Object> getMockedS3Objects() throws Exception {
    obj1 = S3Object.builder().key("obj1").lastModified(Instant.ofEpochMilli(1L)).size(1L).build();
    obj2 = S3Object.builder().key("obj2").lastModified(Instant.ofEpochMilli(2L)).size(2L).build();
    obj3 = S3Object.builder().key("obj3").lastModified(Instant.ofEpochMilli(3L)).size(3L).build();
    obj4 = S3Object.builder().key("obj4").lastModified(Instant.ofEpochMilli(4L)).size(4L).build();
    return List.of(obj1, obj2, obj3, obj4);
  }

  private List<S3Object> getMockedS3ObjectsWithDirectory() throws Exception {
    obj1 = S3Object.builder().key("obj1").lastModified(Instant.ofEpochMilli(1L)).size(1L).build();
    // set obj2 as a directory
    obj2 = S3Object.builder().key("obj2/").lastModified(Instant.ofEpochMilli(2L)).size(2L).build();
    obj3 = S3Object.builder().key("obj3").lastModified(Instant.ofEpochMilli(3L)).size(3L).build();
    obj4 = S3Object.builder().key("obj4").lastModified(Instant.ofEpochMilli(4L)).size(4L).build();
    return List.of(obj1, obj2, obj3, obj4);
  }

  private List<S3Object> getMockedS3JsonObjects() throws Exception {
    obj1 = S3Object.builder().key("obj1.json").lastModified(Instant.ofEpochMilli(1L)).size(1L).build();
    obj2 = S3Object.builder().key("obj2.json").lastModified(Instant.ofEpochMilli(2L)).size(2L).build();
    obj3 = S3Object.builder().key("obj3").lastModified(Instant.ofEpochMilli(3L)).size(3L).build();
    return List.of(obj1, obj2, obj3);
  }

  private List<S3Object> getMockedS3ObjectsWithCompressionAndArchive() throws Exception {
    obj1 = S3Object.builder().key("jsonlCsvAndFolderWithFooTxt.tar").lastModified(Instant.ofEpochMilli(1L)).size(1L).build();
    obj2 = S3Object.builder().key("hello.zip").lastModified(Instant.ofEpochMilli(2L)).size(2L).build();
    obj3 = S3Object.builder().key("textFiles.tar").lastModified(Instant.ofEpochMilli(3L)).size(3L).build();
    obj4 = S3Object.builder().key("jsonlCsvAndFolderWithFooTxt.tar.gz").lastModified(Instant.ofEpochMilli(4L)).size(4L).build();
    S3Object obj5 = S3Object.builder().key("zippedFolder.zip").lastModified(Instant.ofEpochMilli(5L)).size(5L).build();
    S3Object obj6 = S3Object.builder().key("zipped.csv.zip").lastModified(Instant.ofEpochMilli(6L)).size(6L).build();
    return List.of(obj1, obj2, obj3, obj4, obj5, obj6);
  }

  private static List<ResponseInputStream<GetObjectResponse>> buildMockStreamList(String folderPath) throws Exception {
    List<String> fileNames = List.of("jsonlCsvAndFolderWithFooTxt.tar", "hello.zip", "textFiles.tar", "jsonlCsvAndFolderWithFooTxt.tar.gz", "zippedFolder.zip", "zipped.csv.zip");
    List<ResponseInputStream<GetObjectResponse>> mockedStreams = new ArrayList<>();

    for (String fileName : fileNames) {
      File currentFile = new File(folderPath + "/" + fileName);
      BufferedInputStream fileStream = new BufferedInputStream(new FileInputStream(currentFile));
      ResponseInputStream<GetObjectResponse> mockedStream = mock(ResponseInputStream.class);
      when(mockedStream.read(any(byte[].class), anyInt(), anyInt())).thenAnswer(invocation -> fileStream.read(invocation.getArgument(0), invocation.getArgument(1), invocation.getArgument(2)));
      mockedStreams.add(mockedStream);
    }

    return mockedStreams;
  }
}
