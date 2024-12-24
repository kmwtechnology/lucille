package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.GET_FILE_CONTENT;
import static com.kmwllc.lucille.connector.FileConnector.S3_ACCESS_KEY_ID;
import static com.kmwllc.lucille.connector.FileConnector.S3_REGION;
import static com.kmwllc.lucille.connector.FileConnector.S3_SECRET_ACCESS_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import java.util.LinkedHashMap;
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
      verify(jsonFileHandler, times(2)).processFileAndPublish(any(), any(), any());
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

    when(mockClient.listObjectsV2Paginator((ListObjectsV2Request) any())).thenReturn(response);

    String folderPath = new File("src/test/resources/StorageClientTest/testCompressedAndArchived").getAbsolutePath();
    Map<String, byte[]> filesAsBytes = readAllFilesAsBytesWithMap(folderPath);
    when(mockClient.getObjectAsBytes((GetObjectRequest) any()))
      .thenReturn(ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), filesAsBytes.get("jsonlCsvAndFolderWithFooTxt.tar"))
      ).thenReturn(ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), filesAsBytes.get("hello.zip"))
      ).thenReturn(ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), filesAsBytes.get("textFiles.tar"))
      ).thenReturn(ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), filesAsBytes.get("jsonlCsvAndFolderWithFooTxt.tar.gz"))
      ).thenReturn(ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), filesAsBytes.get("zippedFolder.zip"))
      ).thenReturn(ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), filesAsBytes.get("zipped.csv.zip")));

    when(mockClient.listObjectsV2Paginator((ListObjectsV2Request) any())).thenReturn(response);

    s3StorageClient.initializeFileHandlers();
    s3StorageClient.traverse(publisher);

    List<Document> docs = messenger.getDocsSentForProcessing();

    /*
    {id=default.csv-1, source=default.csv, filename=default.csv, field1=a, field2=b, field3=c, csvLineNumber=1, run_id=run1}
    {id=default.csv-2, source=default.csv, filename=default.csv, field1=d, field2=e,f, field3=g, csvLineNumber=2, run_id=run1}
    {id=default.csv-3, source=default.csv, filename=default.csv, field1=x, field2=y, field3=z, csvLineNumber=3, run_id=run1}
    {id=2, imageCredit={artist=Jessica Polar, link=http://www.resplashed.com/photographer/jessica_polar/}, tags=[Person, Woman, Desert], imageUrl=http://www.resplashed.com/img/400_1f92142bd71b.jpg, filename=400_1f92142bd71b.jpg, imageHash=1aaeac2de7c48e4e7773b1f92138291f, price=10.99, name=Gorgeous Woman Mug, description=doloremque architecto ducimus sit nemo voluptatem dolor vel ratione sed quis nostrum et voluptatem quisquam nihil labore recusandae quas nisi rem sit, slug=Gorgeous-Woman-Mug, added=1479995150673, manufacturer=Heathcote-Kautzer-and-Turner, itemType=mug, productImg=mug-400_1f92142bd71b.jpg, run_id=run1}
    {id=3, imageCredit={artist=Lou Levit, link=http://www.resplashed.com/photographer/lou_levit/}, tags=[Cars, City], imageUrl=http://www.resplashed.com/img/400_6812876c6c27.jpg, filename=400_6812876c6c27.jpg, imageHash=63886eb0e3a452da535d175fd1683d05, price=10.99, name=Awesome City Mug, description=et omnis sed facere ab doloribus corrupti esse soluta repudiandae exercitationem impedit ipsum magnam omnis totam quo iure fuga quae sint eligendi culpa possimus et, slug=Awesome-City-Mug-2, added=1481616714312, manufacturer=Dickens-Franecki, itemType=mug, productImg=mug-400_6812876c6c27.jpg, run_id=run1}
    {id=prefix-a917f9309ce20fd85cd87f340e582c97, file_path=FolderWithFooTxt/foo.txt, file_modification_date=2024-12-24T13:58:48Z, file_size_bytes=4, file_content=[B@79ca7bea, run_id=run1}
    {id=prefix-5d41402abc4b2a76b9719d911017c592, file_path=hello, file_modification_date=2024-12-24T14:52:45Z, file_size_bytes=-1, file_content=[B@54f6b629, run_id=run1}
    {id=prefix-0f44513cc7167d999a233c909ac58a14, file_path=helloWorld.txt, file_modification_date=2024-12-24T13:47:07Z, file_size_bytes=12, file_content=[B@4bc9ca97, run_id=run1}
    {id=prefix-24b97e4637e8edc9f34fbb9aaaa71896, file_path=goodbye.txt, file_modification_date=2024-12-24T13:54:56Z, file_size_bytes=0, file_content=[B@3e43f049, run_id=run1}
    {id=default.csv-1, source=default.csv, filename=default.csv, field1=a, field2=b, field3=c, csvLineNumber=1, run_id=run1}
    {id=default.csv-2, source=default.csv, filename=default.csv, field1=d, field2=e,f, field3=g, csvLineNumber=2, run_id=run1}
    {id=default.csv-3, source=default.csv, filename=default.csv, field1=x, field2=y, field3=z, csvLineNumber=3, run_id=run1}
    {id=2, imageCredit={artist=Jessica Polar, link=http://www.resplashed.com/photographer/jessica_polar/}, tags=[Person, Woman, Desert], imageUrl=http://www.resplashed.com/img/400_1f92142bd71b.jpg, filename=400_1f92142bd71b.jpg, imageHash=1aaeac2de7c48e4e7773b1f92138291f, price=10.99, name=Gorgeous Woman Mug, description=doloremque architecto ducimus sit nemo voluptatem dolor vel ratione sed quis nostrum et voluptatem quisquam nihil labore recusandae quas nisi rem sit, slug=Gorgeous-Woman-Mug, added=1479995150673, manufacturer=Heathcote-Kautzer-and-Turner, itemType=mug, productImg=mug-400_1f92142bd71b.jpg, run_id=run1}
    {id=3, imageCredit={artist=Lou Levit, link=http://www.resplashed.com/photographer/lou_levit/}, tags=[Cars, City], imageUrl=http://www.resplashed.com/img/400_6812876c6c27.jpg, filename=400_6812876c6c27.jpg, imageHash=63886eb0e3a452da535d175fd1683d05, price=10.99, name=Awesome City Mug, description=et omnis sed facere ab doloribus corrupti esse soluta repudiandae exercitationem impedit ipsum magnam omnis totam quo iure fuga quae sint eligendi culpa possimus et, slug=Awesome-City-Mug-2, added=1481616714312, manufacturer=Dickens-Franecki, itemType=mug, productImg=mug-400_6812876c6c27.jpg, run_id=run1}
    {id=prefix-a917f9309ce20fd85cd87f340e582c97, file_path=FolderWithFooTxt/foo.txt, file_modification_date=2024-12-24T13:58:48Z, file_size_bytes=4, file_content=[B@147cc940, run_id=run1}
    {id=prefix-443e7fd8e68a53936ade6011c81d7bc5, file_path=zippedFolder/foo.txt, file_modification_date=2024-12-24T13:58:48Z, file_size_bytes=-1, file_content=[B@755a7218, run_id=run1}
    {id=zipped.csv-1, source=zipped.csv, filename=zipped.csv, field1=a, field2=b, field3=c, csvLineNumber=1, run_id=run1}
    {id=zipped.csv-2, source=zipped.csv, filename=zipped.csv, field1=d, field2=e,f, field3=g, csvLineNumber=2, run_id=run1}
    {id=zipped.csv-3, source=zipped.csv, filename=zipped.csv, field1=x, field2=y, field3=z, csvLineNumber=3, run_id=run1}
     */

    assertEquals(19, docs.size());

    // check documents published from jsonlCsvAndFolderWithFooTxt.tar
    Document doc1 = docs.get(0);
    assertEquals("default.csv-1", doc1.getId());
    assertEquals("default.csv", doc1.getString("source"));
    Document doc2 = docs.get(1);
    assertEquals("default.csv-2", doc2.getId());
    assertEquals("default.csv", doc2.getString("source"));
    Document doc3 = docs.get(2);
    assertEquals("default.csv-3", doc3.getId());
    assertEquals("default.csv", doc3.getString("source"));
    Document doc4 = docs.get(3);
    assertEquals("2", doc4.getId());
    assertEquals("Gorgeous Woman Mug", doc4.getString("name"));
    Document doc5 = docs.get(4);
    assertEquals("3", doc5.getId());
    assertEquals("Awesome City Mug", doc5.getString("name"));
    Document doc6 = docs.get(5);
    assertEquals("FolderWithFooTxt/foo.txt", doc6.getString("file_path"));

    // check document published from hello.zip
    Document doc7 = docs.get(6);
    assertEquals("hello", doc7.getString("file_path"));
    // check documents published from textFiles.tar
    Document doc8 = docs.get(7);
    assertEquals("helloWorld.txt", doc8.getString("file_path"));
    Document doc9 = docs.get(8);
    assertEquals("goodbye.txt", doc9.getString("file_path"));
    // check documents published from jsonlCsvAndFolderWithFooTxt.tar.gz
    Document doc10 = docs.get(9);
    assertEquals("default.csv-1", doc10.getId());
    assertEquals("default.csv", doc10.getString("source"));
    Document doc11 = docs.get(10);
    assertEquals("default.csv-2", doc11.getId());
    assertEquals("default.csv", doc11.getString("source"));
    Document doc12 = docs.get(11);
    assertEquals("default.csv-3", doc12.getId());
    assertEquals("default.csv", doc12.getString("source"));
    Document doc13 = docs.get(12);
    assertEquals("2", doc13.getId());
    assertEquals("Gorgeous Woman Mug", doc13.getString("name"));
    Document doc14 = docs.get(13);
    assertEquals("3", doc14.getId());
    assertEquals("Awesome City Mug", doc14.getString("name"));
    Document doc15 = docs.get(14);
    assertEquals("FolderWithFooTxt/foo.txt", doc15.getString("file_path"));
    // check document published from zippedFolder.zip
    Document doc16 = docs.get(15);
    assertEquals("zippedFolder/foo.txt", doc16.getString("file_path"));
    // check documents published from zipped.csv.zip
    Document doc17 = docs.get(16);
    assertEquals("zipped.csv-1", doc17.getId());
    assertEquals("zipped.csv", doc17.getString("source"));
    Document doc18 = docs.get(17);
    assertEquals("zipped.csv-2", doc18.getId());
    assertEquals("zipped.csv", doc18.getString("source"));
    Document doc19 = docs.get(18);
    assertEquals("zipped.csv-3", doc19.getId());
    assertEquals("zipped.csv", doc19.getString("source"));

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
