package com.kmwllc.lucille.connector.storageclients;

import static com.kmwllc.lucille.connector.FileConnector.GET_FILE_CONTENT;
import static com.kmwllc.lucille.connector.FileConnector.S3_ACCESS_KEY_ID;
import static com.kmwllc.lucille.connector.FileConnector.S3_REGION;
import static com.kmwllc.lucille.connector.FileConnector.S3_SECRET_ACCESS_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.core.fileHandlers.FileTypeHandler;
import com.kmwllc.lucille.core.fileHandlers.JsonFileTypeHandler;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.Test;
import org.mockito.MockedStatic;
import software.amazon.awssdk.core.ResponseBytes;
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
  public void testShutdown() throws Exception {
    // test if region is set but of non-existent region
    Map<String, Object> cloudOptions = Map.of(S3_REGION, "us-east-1", S3_ACCESS_KEY_ID, "accessKey",
        S3_SECRET_ACCESS_KEY, "secretKey");
    S3StorageClient s3StorageClient = new S3StorageClient(new URI("s3://bucket/"), null, null,
        null, null, cloudOptions, ConfigFactory.empty());

    S3Client mockClient = mock(S3Client.class);
    s3StorageClient.setS3ClientForTesting(mockClient);
    s3StorageClient.shutdown();
    verify(mockClient, times(1)).close();
  }

  @Test
  public void testPublishValidFiles() throws Exception {
    Map<String, Object> cloudOptions = Map.of(S3_REGION, "us-east-1", S3_ACCESS_KEY_ID, "accessKey",
        S3_SECRET_ACCESS_KEY, "secretKey");
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.parseMap(Map.of());
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    S3StorageClient s3StorageClient = new S3StorageClient(new URI("s3://bucket/"), publisher, "prefix-",
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

    s3StorageClient.publishFiles();

    List<Document> documents = messenger.getDocsSentForProcessing();

    // all documents processed
    assertEquals(4, documents.size());
    Document doc1 = documents.get(0);
    assertTrue(doc1.getId().startsWith("prefix-"));
    assertEquals("s3://bucket/obj1", doc1.getString("file_path"));
    assertEquals("1", doc1.getString("file_size_bytes"));
    assertArrayEquals(new byte[]{1, 2, 3, 4}, doc1.getBytes("file_content"));

    Document doc2 = documents.get(1);
    assertTrue(doc2.getId().startsWith("prefix-"));
    assertEquals("s3://bucket/obj2", doc2.getString("file_path"));
    assertEquals("2", doc2.getString("file_size_bytes"));
    assertArrayEquals(new byte[]{5, 6, 7, 8}, doc2.getBytes("file_content"));

    Document doc3 = documents.get(2);
    assertTrue(doc3.getId().startsWith("prefix-"));
    assertEquals("s3://bucket/obj3", doc3.getString("file_path"));
    assertEquals("3", doc3.getString("file_size_bytes"));
    assertArrayEquals(new byte[]{9, 10, 11, 12}, doc3.getBytes("file_content"));

    Document doc4 = documents.get(3);
    assertTrue(doc4.getId().startsWith("prefix-"));
    assertEquals("s3://bucket/obj4", doc4.getString("file_path"));
    assertEquals("4", doc4.getString("file_size_bytes"));
    assertArrayEquals(new byte[]{13, 14, 15, 16}, doc4.getBytes("file_content"));
  }


  @Test
  public void testPublishInvalidFiles() throws Exception {
    Map<String, Object> cloudOptions = Map.of(S3_REGION, "us-east-1", S3_ACCESS_KEY_ID, "accessKey",
        S3_SECRET_ACCESS_KEY, "secretKey");
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.parseMap(Map.of());
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    S3StorageClient s3StorageClient = new S3StorageClient(new URI("s3://bucket/"), publisher, "prefix-",
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

    s3StorageClient.publishFiles();

    List<Document> documents = messenger.getDocsSentForProcessing();
    // only obj1 processed, skipping over directory obj2, and obj3 and obj4 because of exclude regex
    assertEquals(1, documents.size());
    Document doc1 = documents.get(0);
    assertTrue(doc1.getId().startsWith("prefix-"));
    assertEquals("s3://bucket/obj1", doc1.getString("file_path"));
    assertEquals("1", doc1.getString("file_size_bytes"));
    assertArrayEquals(new byte[]{1, 2, 3, 4}, doc1.getBytes("file_content"));
  }

  @Test
  public void testSkipFileContent() throws Exception {
    Map<String, Object> cloudOptions = Map.of(S3_REGION, "us-east-1", S3_ACCESS_KEY_ID, "accessKey",
        S3_SECRET_ACCESS_KEY, "secretKey");
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.parseMap(Map.of());
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    S3StorageClient s3StorageClient = new S3StorageClient(new URI("s3://bucket/"), publisher, "prefix-",
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

    s3StorageClient.publishFiles();

    List<Document> documents = messenger.getDocsSentForProcessing();

    // check that only one document published without file_content
    assertEquals(1, documents.size());
    assertNull(documents.get(0).getBytes("file_content"));
  }

  @Test
  public void testPublishUsingFileHandler() throws Exception {
    Map<String, Object> cloudOptions = Map.of(S3_REGION, "us-east-1", S3_ACCESS_KEY_ID, "accessKey",
        S3_SECRET_ACCESS_KEY, "secretKey");
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.parseMap(Map.of());
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    // storage client with json file handler
    S3StorageClient s3StorageClient = new S3StorageClient(new URI("s3://bucket/"), publisher, "prefix-",
        List.of(), List.of(), cloudOptions, ConfigFactory.parseMap(Map.of("json", Map.of())));

    S3Client mockClient = mock(S3Client.class, RETURNS_DEEP_STUBS);
    s3StorageClient.setS3ClientForTesting(mockClient);
    ListObjectsV2Iterable response = mock(ListObjectsV2Iterable.class);
    ListObjectsV2Response responseWithinStream = mock(ListObjectsV2Response.class);
    when(responseWithinStream.contents()).thenReturn(getMockedS3JsonObjects());
    when(response.stream()).thenReturn(Stream.of(responseWithinStream));

    when(mockClient.listObjectsV2Paginator((ListObjectsV2Request) any())).thenReturn(response);

    try (MockedStatic<FileTypeHandler> mockFileHandler = mockStatic(FileTypeHandler.class)) {
      FileTypeHandler jsonFileTypeHandler = mock(JsonFileTypeHandler.class);
      mockFileHandler.when(() -> FileTypeHandler.getNewFileTypeHandler(any(), any()))
          .thenReturn(jsonFileTypeHandler);
      mockFileHandler.when(() -> FileTypeHandler.supportAndContainFileType(any(), any()))
          .thenReturn(true).thenReturn(true).thenReturn(false);

      s3StorageClient.initializeFileHandlers();
      s3StorageClient.publishFiles();
      // verify that the processFileAndPublish is only called for the json files
      verify(jsonFileTypeHandler, times(2)).processFileAndPublish(any(), any(), any());
    }

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

}
