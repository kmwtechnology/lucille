package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.ARCHIVE_FILE_SEPARATOR;
import static com.kmwllc.lucille.connector.FileConnector.CONTENT;
import static com.kmwllc.lucille.connector.FileConnector.FILE_PATH;
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
import com.kmwllc.lucille.core.fileHandler.FileHandler;
import com.kmwllc.lucille.core.fileHandler.JsonFileHandler;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

public class GoogleStorageClientTest {

  Storage storage = LocalStorageHelper.getOptions().getService();

  @Test
  public void testInvalidPathToServiceKey() throws Exception {
    Config cloudOptions = ConfigFactory.parseMap(Map.of(GOOGLE_SERVICE_KEY, "invalidPath"));
    GoogleStorageClient googleStorageClient = new GoogleStorageClient(cloudOptions);

    assertThrows(IOException.class, googleStorageClient::init);
  }

  @Test
  public void testPublishValidFiles() throws Exception {
    Config cloudOptions = ConfigFactory.parseMap(Map.of(GOOGLE_SERVICE_KEY, "validPath"));
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.parseMap(Map.of());
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");
    GoogleStorageClient googleStorageClient = new GoogleStorageClient(cloudOptions);
    TraversalParams params = new TraversalParams(new URI("gs://bucket/"), "prefix-", ConfigFactory.empty(), ConfigFactory.empty());

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
    googleStorageClient.initializeForTesting();
    googleStorageClient.traverse(publisher, params);

    // validate that all 4 blobs are published
    List<Document> documents = messenger.getDocsSentForProcessing();
    assertEquals(4, documents.size());

    // validate that file_path is correct
    assertEquals("gs://bucket/my-object", documents.get(3).getString(FILE_PATH));
    assertEquals("gs://bucket/my-object2", documents.get(0).getString(FILE_PATH));
    assertEquals("gs://bucket/my-object3", documents.get(1).getString(FILE_PATH));
    assertEquals("gs://bucket/my-object4", documents.get(2).getString(FILE_PATH));

    // validate that the content of the documents is correct
    assertEquals("Hello, World!", new String(documents.get(3).getBytes(CONTENT)));
    assertEquals("Hello!", new String(documents.get(0).getBytes(CONTENT)));
    assertEquals("World!", new String(documents.get(1).getBytes(CONTENT)));
    assertEquals("foo", new String(documents.get(2).getBytes(CONTENT)));

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

    // closes storage too
    googleStorageClient.shutdown();
  }

  @Test
  public void testSkipFileContent() throws Exception {
    Config cloudOptions = ConfigFactory.parseMap(Map.of(GOOGLE_SERVICE_KEY, "validPath"));
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.parseMap(Map.of());
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");
    GoogleStorageClient googleStorageClient = new GoogleStorageClient(cloudOptions);
    TraversalParams params = new TraversalParams(new URI("gs://bucket/"), "prefix-", ConfigFactory.parseMap(Map.of(GET_FILE_CONTENT, false)), ConfigFactory.empty());

    BlobId blobId = BlobId.of("bucket", "my-object");
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    storage.create(blobInfo, "Hello, World!".getBytes());

    googleStorageClient.setStorageForTesting(storage);
    googleStorageClient.initializeForTesting();
    googleStorageClient.traverse(publisher, params);

    List<Document> documents = messenger.getDocsSentForProcessing();
    assertEquals(1, documents.size());
    assertEquals("gs://bucket/my-object", documents.get(0).getString(FILE_PATH));
    assertFalse(documents.get(0).has(CONTENT));

    // closes storage too
    googleStorageClient.shutdown();
  }

  @Test
  public void testTraverseWithExcludes() throws Exception {
    Config cloudOptions = ConfigFactory.parseMap(Map.of(GOOGLE_SERVICE_KEY, "validPath"));
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.parseMap(Map.of());
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");
    GoogleStorageClient googleStorageClient = new GoogleStorageClient(cloudOptions);
    Map<String, Object> filterOptionsMap = Map.of("excludes", List.of("my-object2", "my-object3"));
    TraversalParams params = new TraversalParams(new URI("gs://bucket/"), "prefix-", ConfigFactory.empty(), ConfigFactory.parseMap(filterOptionsMap));

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
    googleStorageClient.initializeForTesting();
    googleStorageClient.traverse(publisher, params);

    // validate that only 2 blob are published and none are object2 or object3
    List<Document> documents = messenger.getDocsSentForProcessing();
    assertEquals(2, documents.size());
    assertEquals("gs://bucket/my-object4", documents.get(0).getString(FILE_PATH));
    assertEquals("gs://bucket/my-object", documents.get(1).getString(FILE_PATH));

    // closes storage too
    googleStorageClient.shutdown();
  }

  @Test
  public void testPublishUsingFileHandler() throws Exception {
    Config cloudOptions = ConfigFactory.parseMap(Map.of(GOOGLE_SERVICE_KEY, "path/to/service/key"));
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
    GoogleStorageClient gStorageClient = new GoogleStorageClient(cloudOptions);
    TraversalParams params = new TraversalParams(new URI("gs://bucket/"), "prefix-", ConfigFactory.parseMap(Map.of("json", Map.of())), ConfigFactory.empty());
    gStorageClient.setStorageForTesting(storage);

    try (MockedStatic<FileHandler> mockFileHandler = mockStatic(FileHandler.class)) {
      FileHandler jsonFileHandler = mock(JsonFileHandler.class);
      mockFileHandler.when(() -> FileHandler.createFromConfig(any())).thenReturn(Map.of("json", jsonFileHandler));
      mockFileHandler.when(() -> FileHandler.supportAndContainFileType(any(), any()))
          .thenReturn(true).thenReturn(false).thenReturn(true); // .json, then object3, then .json

      gStorageClient.initializeForTesting();
      gStorageClient.traverse(publisher, params);
      // verify that the processFileAndPublish is only called twice for the 2 json files out of the 3 files
      ArgumentCaptor<String> fileNameCaptor = ArgumentCaptor.forClass(String.class);
      verify(jsonFileHandler, times(2)).processFileAndPublish(any(), any(), fileNameCaptor.capture());
      List<String> capturedFileNames = fileNameCaptor.getAllValues();
      assertEquals("gs://bucket/json-1.json", capturedFileNames.get(0));
      assertEquals("gs://bucket/json-2.json", capturedFileNames.get(1));
    }

    // closes storage too
    gStorageClient.shutdown();
  }

  @Test
  public void testPublishOnCompressedAndArchived() throws Exception {
    Config cloudOptions = ConfigFactory.parseMap(Map.of(GOOGLE_SERVICE_KEY, "validPath"));
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.parseMap(Map.of());
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");
    GoogleStorageClient googleStorageClient = new GoogleStorageClient(cloudOptions);

    TraversalParams params = new TraversalParams(new URI("gs://bucket/"), "prefix-",
        ConfigFactory.parseMap(
            Map.of(
                "json", Map.of(),
                "csv", Map.of(),
                "handleArchivedFiles", true,
                "handleCompressedFiles", true
            )
        ), ConfigFactory.empty());

    Map<String, byte[]> fileContents = readAllFilesAsBytesWithMap("src/test/resources/StorageClientTest/testCompressedAndArchived");

    BlobId blobId = BlobId.of("bucket", "jsonlCsvAndFolderWithFooTxt.tar");
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    storage.create(blobInfo, fileContents.get("jsonlCsvAndFolderWithFooTxt.tar"));

    BlobId blobId2 = BlobId.of("bucket", "hello.zip");
    BlobInfo blobInfo2 = BlobInfo.newBuilder(blobId2).build();
    storage.create(blobInfo2, fileContents.get("hello.zip"));

    BlobId blobId3 = BlobId.of("bucket", "textFiles.tar");
    BlobInfo blobInfo3 = BlobInfo.newBuilder(blobId3).build();
    storage.create(blobInfo3, fileContents.get("textFiles.tar"));

    BlobId blobId4 = BlobId.of("bucket", "jsonlCsvAndFolderWithFooTxt.tar.gz");
    BlobInfo blobInfo4 = BlobInfo.newBuilder(blobId4).build();
    storage.create(blobInfo4, fileContents.get("jsonlCsvAndFolderWithFooTxt.tar.gz"));

    BlobId blobId5 = BlobId.of("bucket", "zippedFolder.zip");
    BlobInfo blobInfo5 = BlobInfo.newBuilder(blobId5).build();
    storage.create(blobInfo5, fileContents.get("zippedFolder.zip"));

    BlobId blobId6 = BlobId.of("bucket", "zipped.csv.zip");
    BlobInfo blobInfo6 = BlobInfo.newBuilder(blobId6).build();
    storage.create(blobInfo6, fileContents.get("zipped.csv.zip"));

    googleStorageClient.setStorageForTesting(storage);

    googleStorageClient.initializeForTesting();
    googleStorageClient.traverse(publisher, params);
    List<Document> docs = messenger.getDocsSentForProcessing();

    assertEquals(19, docs.size());

    // check documents published from jsonlCsvAndFolderWithFooTxt.tar
    Document doc1 = docs.get(0);
    assertEquals("default.csv-1", doc1.getId());
    assertEquals("default.csv", doc1.getString("filename"));
    assertEquals("gs://bucket/jsonlCsvAndFolderWithFooTxt.tar" + ARCHIVE_FILE_SEPARATOR + "default.csv", doc1.getString("source"));
    Document doc2 = docs.get(1);
    assertEquals("default.csv-2", doc2.getId());
    assertEquals("default.csv", doc2.getString("filename"));
    assertEquals("gs://bucket/jsonlCsvAndFolderWithFooTxt.tar" + ARCHIVE_FILE_SEPARATOR + "default.csv", doc2.getString("source"));
    Document doc3 = docs.get(2);
    assertEquals("default.csv-3", doc3.getId());
    assertEquals("default.csv", doc3.getString("filename"));
    assertEquals("gs://bucket/jsonlCsvAndFolderWithFooTxt.tar" + ARCHIVE_FILE_SEPARATOR + "default.csv", doc3.getString("source"));
    Document doc4 = docs.get(3);
    assertEquals("2", doc4.getId());
    assertEquals("Gorgeous Woman Mug", doc4.getString("name"));
    Document doc5 = docs.get(4);
    assertEquals("3", doc5.getId());
    assertEquals("Awesome City Mug", doc5.getString("name"));
    Document doc6 = docs.get(5);
    assertEquals("gs://bucket/jsonlCsvAndFolderWithFooTxt.tar" + ARCHIVE_FILE_SEPARATOR + "FolderWithFooTxt/foo.txt", doc6.getString(FILE_PATH));

    // check documents published from textFiles.tar
    Document doc7 = docs.get(6);
    assertEquals("gs://bucket/textFiles.tar" + ARCHIVE_FILE_SEPARATOR + "helloWorld.txt", doc7.getString(FILE_PATH));
    Document doc8 = docs.get(7);
    assertEquals("gs://bucket/textFiles.tar" + ARCHIVE_FILE_SEPARATOR + "goodbye.txt", doc8.getString(FILE_PATH));

    // check documents published from jsonlCsvAndFolderWithFooTxt.tar.gz
    Document doc9 = docs.get(8);
    assertEquals("default.csv-1", doc9.getId());
    assertEquals("default.csv", doc9.getString("filename"));
    assertEquals("gs://bucket/jsonlCsvAndFolderWithFooTxt.tar.gz" + ARCHIVE_FILE_SEPARATOR + "default.csv", doc9.getString("source"));
    Document doc10 = docs.get(9);
    assertEquals("default.csv-2", doc10.getId());
    assertEquals("default.csv", doc10.getString("filename"));
    assertEquals("gs://bucket/jsonlCsvAndFolderWithFooTxt.tar.gz" + ARCHIVE_FILE_SEPARATOR + "default.csv", doc10.getString("source"));
    Document doc11 = docs.get(10);
    assertEquals("default.csv-3", doc11.getId());
    assertEquals("default.csv", doc11.getString("filename"));
    assertEquals("gs://bucket/jsonlCsvAndFolderWithFooTxt.tar.gz" + ARCHIVE_FILE_SEPARATOR + "default.csv", doc11.getString("source"));
    Document doc12 = docs.get(11);
    assertEquals("2", doc12.getId());
    assertEquals("Gorgeous Woman Mug", doc12.getString("name"));
    Document doc13 = docs.get(12);
    assertEquals("3", doc13.getId());
    assertEquals("Awesome City Mug", doc13.getString("name"));
    Document doc14 = docs.get(13);
    assertEquals("gs://bucket/jsonlCsvAndFolderWithFooTxt.tar.gz" + ARCHIVE_FILE_SEPARATOR + "FolderWithFooTxt/foo.txt", doc14.getString(FILE_PATH));
    // check documents published from zipped.csv.zip
    Document doc15 = docs.get(14);
    assertEquals("zipped.csv-1", doc15.getId());
    assertEquals("zipped.csv", doc15.getString("filename"));
    assertEquals("gs://bucket/zipped.csv.zip" + ARCHIVE_FILE_SEPARATOR + "zipped.csv", doc15.getString("source"));
    Document doc16 = docs.get(15);
    assertEquals("zipped.csv-2", doc16.getId());
    assertEquals("zipped.csv", doc16.getString("filename"));
    assertEquals("gs://bucket/zipped.csv.zip" + ARCHIVE_FILE_SEPARATOR + "zipped.csv", doc16.getString("source"));
    Document doc17 = docs.get(16);
    assertEquals("zipped.csv-3", doc17.getId());
    assertEquals("zipped.csv", doc17.getString("filename"));
    assertEquals("gs://bucket/zipped.csv.zip" + ARCHIVE_FILE_SEPARATOR + "zipped.csv", doc17.getString("source"));
    // check documents published from zippedFolder.zip
    Document doc18 = docs.get(17);
    assertEquals("gs://bucket/zippedFolder.zip" + ARCHIVE_FILE_SEPARATOR + "zippedFolder/foo.txt", doc18.getString(FILE_PATH));
    // check documents published from hello.zip
    Document doc19 = docs.get(18);
    assertEquals("gs://bucket/hello.zip" + ARCHIVE_FILE_SEPARATOR + "hello", doc19.getString(FILE_PATH));


    // closes storage too
    googleStorageClient.shutdown();
  }

  @Test
  public void testErrorMovingFiles() throws Exception {
    Config cloudOptions = ConfigFactory.parseMap(Map.of(GOOGLE_SERVICE_KEY, "validPath"));
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.parseMap(Map.of());
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");
    GoogleStorageClient googleStorageClient = new GoogleStorageClient(cloudOptions);
    TraversalParams params = new TraversalParams(new URI("gs://bucket/"), "prefix-",
        ConfigFactory.parseMap(
            Map.of(
                "moveToAfterProcessing", "gs://bucket/processed",
                "moveToErrorFolder", "gs://bucket/error"
            )
        ), ConfigFactory.empty());


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
    googleStorageClient.initializeForTesting();
    assertThrows(UnsupportedOperationException.class, () -> googleStorageClient.traverse(publisher, params));

    // closes storage too
    googleStorageClient.shutdown();
  }

  @Test
  public void testGetFileContentStream() throws Exception {
    GoogleStorageClient storageClient = new GoogleStorageClient(
        ConfigFactory.parseMap(Map.of(GOOGLE_SERVICE_KEY, "validPath")));
    URI testURI = new URI("gs://bucket/hello.txt");

    BlobId blobId = BlobId.of("bucket", "hello.txt");
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    storage.create(blobInfo, "Hello there.".getBytes());

    storageClient.setStorageForTesting(storage);
    storageClient.initializeForTesting();

    InputStream results = storageClient.getFileContentStream(testURI);

    assertEquals("Hello there.", new String(results.readAllBytes()));

    storageClient.shutdown();
  }

  // ** NOTE: There is not a test for the modificationCutoff here. Unfortunately, there doesn't appear to be an easy way
  // to create objects with a certain modification time (including trying to do so via Mockito). If this somehow changes
  // in the future it would be worth a revisit. **

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
