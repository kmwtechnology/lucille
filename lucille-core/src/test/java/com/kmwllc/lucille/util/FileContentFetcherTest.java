package com.kmwllc.lucille.util;

import static com.kmwllc.lucille.connector.FileConnector.S3_ACCESS_KEY_ID;
import static com.kmwllc.lucille.connector.FileConnector.S3_REGION;
import static com.kmwllc.lucille.connector.FileConnector.S3_SECRET_ACCESS_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.connector.storageclient.S3StorageClient;
import com.kmwllc.lucille.core.ConnectorException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Map;
import org.junit.Test;
import org.mockito.MockedConstruction;

public class FileContentFetcherTest {

  @Test
  public void testGetInputStream() throws Exception {
    FileContentFetcher fetcher = new FileContentFetcher(Map.of());
    fetcher.startup();

    // 1. Classpath file
    String path = "classpath:FileContentFetcherTest/hello.txt";
    try (InputStream stream = fetcher.getInputStream(path)) {
      assertEquals("Hello there.", new String(stream.readAllBytes()));
    }

    // 2. URI - using a StorageClient
    path = Paths.get("src/test/resources/FileContentFetcherTest/hello.txt").toUri().toString();
    try (InputStream stream = fetcher.getInputStream(path)) {
      assertEquals("Hello there.", new String(stream.readAllBytes()));
    }

    // 3. Local file path
    path = Paths.get("src/test/resources/FileContentFetcherTest/hello.txt").toString();
    try (InputStream stream = fetcher.getInputStream(path)) {
      assertEquals("Hello there.", new String(stream.readAllBytes()));
    }

    // 4. Invalid URI (no S3StorageClient included here)
    assertThrows(IOException.class, () -> fetcher.getInputStream("s3://bucket/hello.txt"));
  }

  @Test
  public void testMultipleStorageClients() throws Exception {
    Map<String, Object> cloudOptions = Map.of(
        S3_REGION, "us-east-1",
        S3_ACCESS_KEY_ID, "accessKey",
        S3_SECRET_ACCESS_KEY, "secretKey");

    URI s3URI = URI.create("s3://bucket/hello.txt");

    // Mocking construction - just want to make sure that the fetcher is appropriately deferring to each storage client
    // as appropriate based on the strings it is supplied.
    try (MockedConstruction<S3StorageClient> mockedConstruction = mockConstruction(S3StorageClient.class, (mock, context) -> {
      when(mock.getFileContentStream(s3URI)).thenReturn(new ByteArrayInputStream("Hello there - S3.".getBytes()));
    })) {
      FileContentFetcher fetcher = new FileContentFetcher(cloudOptions);

      URI localPathURI = Paths.get("src/test/resources/FileContentFetcherTest/hello.txt").toUri();
      InputStream localInputStream = fetcher.getInputStream(localPathURI.toString());
      assertEquals("Hello there.", new String(localInputStream.readAllBytes()));

      InputStream s3MockInputStream = fetcher.getInputStream(s3URI.toString());
      assertEquals("Hello there - S3.", new String(s3MockInputStream.readAllBytes()));

      assertThrows(IOException.class, () -> fetcher.getInputStream("gs://bucket/hello.txt"));
    }
  }

  @Test
  public void testClientFailsInit() throws Exception {
    Map<String, Object> cloudOptions = Map.of(
        S3_REGION, "us-east-1",
        S3_ACCESS_KEY_ID, "accessKey",
        S3_SECRET_ACCESS_KEY, "secretKey");

    try (MockedConstruction<S3StorageClient> mockedConstruction = mockConstruction(S3StorageClient.class, (mock, context) -> {
      doThrow(new ConnectorException("Mock Init Exception")).when(mock).init();
      doThrow(new IOException("Mock Shutdown Exception")).when(mock).shutdown();
    })) {
      FileContentFetcher fetcher = new FileContentFetcher(cloudOptions);
      assertThrows(IOException.class, () -> fetcher.startup());

      // an error in fetcher.startup() should call shutdown automatically
      S3StorageClient mockS3 = mockedConstruction.constructed().get(0);
      verify(mockS3, times(1)).shutdown();
    }
  }

  @Test
  public void testStaticFetches() throws Exception {
    Map<String, Object> cloudOptions = Map.of(
        S3_REGION, "us-east-1",
        S3_ACCESS_KEY_ID, "accessKey",
        S3_SECRET_ACCESS_KEY, "secretKey");

    URI s3URI = URI.create("s3://bucket/hello.txt");

    // Mocking construction - just want to make sure that the fetcher is appropriately deferring to each storage client
    // as appropriate based on the strings it is supplied.
    try (MockedConstruction<S3StorageClient> mockedConstruction = mockConstruction(S3StorageClient.class, (mock, context) -> {
      when(mock.getFileContentStream(s3URI)).thenReturn(new ByteArrayInputStream("Hello there - S3.".getBytes()));
    })) {
      InputStream s3MockInputStream = FileContentFetcher.getSingleInputStream(s3URI.toString(), cloudOptions);

      S3StorageClient mockedClient = mockedConstruction.constructed().get(0);
      verify(mockedClient, times(0)).shutdown();

      assertEquals("Hello there - S3.", new String(s3MockInputStream.readAllBytes()));
      s3MockInputStream.close();
      verify(mockedClient, times(1)).shutdown();

      URI localPathURI = Paths.get("src/test/resources/FileContentFetcherTest/hello.txt").toUri();
      InputStream localInputStream = FileContentFetcher.getSingleInputStream(localPathURI.toString(), cloudOptions);
      assertEquals("Hello there.", new String(localInputStream.readAllBytes()));

      // Google should still be unavailable
      assertThrows(IOException.class, () -> FileContentFetcher.getSingleInputStream("gs://bucket/hello.txt", cloudOptions));
    }
  }
}
