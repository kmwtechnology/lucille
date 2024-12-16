package com.kmwllc.lucille.connector.storageclients;

import static com.kmwllc.lucille.connector.FileConnector.GET_FILE_CONTENT;
import static com.kmwllc.lucille.connector.FileConnector.GOOGLE_SERVICE_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.core.fileHandlers.FileTypeHandler;
import com.kmwllc.lucille.core.fileHandlers.JsonFileTypeHandler;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.Test;
import org.mockito.MockedStatic;

public class GoogleStorageClientTest {

  Storage storage = LocalStorageHelper.getOptions().getService();

  @Test
  public void testInvalidPathToServiceKey() throws Exception {
    Map<String, Object> cloudOptions = Map.of(GOOGLE_SERVICE_KEY, "invalidPath");
    GoogleStorageClient googleStorageClient = new GoogleStorageClient(new URI("gs://bucket/"), null, null,
        null, null, cloudOptions, ConfigFactory.empty());

    assertThrows(IllegalArgumentException.class, googleStorageClient::init);
  }

  @Test
  public void testShutdown() throws Exception {
    Map<String, Object> cloudOptions = Map.of(GOOGLE_SERVICE_KEY, "validPath");
    GoogleStorageClient googleStorageClient = new GoogleStorageClient(new URI("gs://bucket/"), null, null,
        null, null, cloudOptions, ConfigFactory.empty());
    Storage mockStorage = mock(Storage.class);
    googleStorageClient.setStorageForTesting(mockStorage);
    googleStorageClient.shutdown();

    // verify that storage.close() is called
    verify(mockStorage, times(1)).close();
  }

  @Test
  public void testPublishValidFiles() throws Exception {
    Map<String, Object> cloudOptions = Map.of(GOOGLE_SERVICE_KEY, "validPath");
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.parseMap(Map.of());
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");
    GoogleStorageClient googleStorageClient = new GoogleStorageClient(new URI("gs://bucket/"), publisher, "prefix-",
        List.of(), List.of(), cloudOptions, ConfigFactory.empty());

    BlobId blobId = BlobId.of("bucket", "my-object");
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    storage.create(blobInfo, "Hello, World!".getBytes());

    BlobId blobId2 = BlobId.of("bucket", "my-object2");
    BlobInfo blobInfo2 = BlobInfo.newBuilder(blobId2).build();
    storage.create(blobInfo2, "Hello!".getBytes());

    BlobId blobId3 = BlobId.of("bucket", "my-object3");
    BlobInfo blobInfo3 = BlobInfo.newBuilder(blobId3).build();
    storage.create(blobInfo3, "World!".getBytes());

    BlobId blobId4 = BlobId.of("bucket", "my-object4");
    BlobInfo blobInfo4 = BlobInfo.newBuilder(blobId4).build();
    storage.create(blobInfo4, "foo".getBytes());

    googleStorageClient.setStorageForTesting(storage);
    googleStorageClient.publishFiles();

    // validate that all 4 blobs are published
    List<Document> documents = messenger.getDocsSentForProcessing();
    assertEquals(4, documents.size());

    // validate that file_path is correct
    assertEquals("gs://bucket/my-object", documents.get(3).getString("file_path"));
    assertEquals("gs://bucket/my-object2", documents.get(0).getString("file_path"));
    assertEquals("gs://bucket/my-object3", documents.get(1).getString("file_path"));
    assertEquals("gs://bucket/my-object4", documents.get(2).getString("file_path"));

    // validate that the content of the documents is correct
    assertEquals("Hello, World!", new String(documents.get(3).getBytes("file_content")));
    assertEquals("Hello!", new String(documents.get(0).getBytes("file_content")));
    assertEquals("World!", new String(documents.get(1).getBytes("file_content")));
    assertEquals("foo", new String(documents.get(2).getBytes("file_content")));

    // validate that the docIdPrefix is correct
    assertTrue(documents.get(3).getId().startsWith("prefix-"));
    assertTrue(documents.get(0).getId().startsWith("prefix-"));
    assertTrue(documents.get(1).getId().startsWith("prefix-"));
    assertTrue(documents.get(2).getId().startsWith("prefix-"));


    // validate that the size is correct
    assertEquals("13", documents.get(3).getString("file_size_bytes"));
    assertEquals("6", documents.get(0).getString("file_size_bytes"));
    assertEquals("6", documents.get(1).getString("file_size_bytes"));
    assertEquals("3", documents.get(2).getString("file_size_bytes"));
  }

  @Test
  public void testSkipFileContent() throws Exception {
    Map<String, Object> cloudOptions = Map.of(GOOGLE_SERVICE_KEY, "validPath");
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.parseMap(Map.of());
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");
    GoogleStorageClient googleStorageClient = new GoogleStorageClient(new URI("gs://bucket/"), publisher, "prefix-",
        List.of(), List.of(), cloudOptions, ConfigFactory.parseMap(Map.of(GET_FILE_CONTENT, false)));

    BlobId blobId = BlobId.of("bucket", "my-object");
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    storage.create(blobInfo, "Hello, World!".getBytes());

    googleStorageClient.setStorageForTesting(storage);
    googleStorageClient.publishFiles();

    List<Document> documents = messenger.getDocsSentForProcessing();
    assertEquals(1, documents.size());
    assertEquals("gs://bucket/my-object", documents.get(0).getString("file_path"));
    assertFalse(documents.get(0).has("file_content"));
  }

  @Test
  public void testPublishFilesWithSomeInvalid() throws Exception {
    Map<String, Object> cloudOptions = Map.of(GOOGLE_SERVICE_KEY, "validPath");
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.parseMap(Map.of());
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");
    GoogleStorageClient googleStorageClient = new GoogleStorageClient(new URI("gs://bucket/"), publisher, "prefix-",
        List.of(Pattern.compile("my-object2"), Pattern.compile("my-object3")), List.of(), cloudOptions, ConfigFactory.empty());

    BlobId blobId = BlobId.of("bucket", "my-object");
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    storage.create(blobInfo, "Hello, World!".getBytes());

    BlobId blobId2 = BlobId.of("bucket", "my-object2");
    BlobInfo blobInfo2 = BlobInfo.newBuilder(blobId2).build();
    storage.create(blobInfo2, "Hello!".getBytes());

    BlobId blobId3 = BlobId.of("bucket", "my-object3");
    BlobInfo blobInfo3 = BlobInfo.newBuilder(blobId3).build();
    storage.create(blobInfo3, "World!".getBytes());

    BlobId blobId4 = BlobId.of("bucket", "my-object4");
    BlobInfo blobInfo4 = BlobInfo.newBuilder(blobId4).build();
    storage.create(blobInfo4, "foo".getBytes());

    googleStorageClient.setStorageForTesting(storage);
    googleStorageClient.publishFiles();

    // validate that only 2 blob are published and none are object2 or object3
    List<Document> documents = messenger.getDocsSentForProcessing();
    assertEquals(2, documents.size());
    assertEquals("gs://bucket/my-object4", documents.get(0).getString("file_path"));
    assertEquals("gs://bucket/my-object", documents.get(1).getString("file_path"));
  }

  @Test
  public void testPublishUsingFileHandler() throws Exception {
    Map<String, Object> cloudOptions = Map.of(GOOGLE_SERVICE_KEY, "path/to/service/key");
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.parseMap(Map.of());
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    BlobId blobId = BlobId.of("bucket", "json-1.json");
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    storage.create(blobInfo, "Hello, World!".getBytes());

    BlobId blobId2 = BlobId.of("bucket", "json-2.json");
    BlobInfo blobInfo2 = BlobInfo.newBuilder(blobId2).build();
    storage.create(blobInfo2, "Hello!".getBytes());

    // non Json blob
    BlobId blobId3 = BlobId.of("bucket", "my-object3");
    BlobInfo blobInfo3 = BlobInfo.newBuilder(blobId3).build();
    storage.create(blobInfo3, "World!".getBytes());


    // google storage client
    GoogleStorageClient gStorageClient = new GoogleStorageClient(new URI("gs://bucket/"), publisher, "prefix-",
        List.of(), List.of(), cloudOptions, ConfigFactory.parseMap(Map.of("json", Map.of())));
    gStorageClient.setStorageForTesting(storage);

    try (MockedStatic<FileTypeHandler> mockFileHandler = mockStatic(FileTypeHandler.class)) {
      FileTypeHandler jsonFileTypeHandler = mock(JsonFileTypeHandler.class);
      mockFileHandler.when(() -> FileTypeHandler.getNewFileTypeHandler(any(), any()))
          .thenReturn(jsonFileTypeHandler);
      mockFileHandler.when(() -> FileTypeHandler.supportAndContainFileType(any(), any()))
          .thenReturn(true).thenReturn(false).thenReturn(true); // .json, then object3, then .json

      gStorageClient.initializeFileHandlers();
      gStorageClient.publishFiles();
      // verify that the processFileAndPublish is only called for the json files
      verify(jsonFileTypeHandler, times(2)).processFileAndPublish(any(), any(), any());
    }

    gStorageClient.shutdown();
  }
}
