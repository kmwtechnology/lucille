package com.kmwllc.lucille.util;

import static com.kmwllc.lucille.connector.FileConnector.GOOGLE_SERVICE_KEY;
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

import com.kmwllc.lucille.connector.storageclient.GoogleStorageClient;
import com.kmwllc.lucille.connector.storageclient.S3StorageClient;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
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
    FileContentFetcher fetcher = new FileContentFetcher(ConfigFactory.empty());
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
    Config cloudOptions = ConfigFactory.parseMap(Map.of(
        S3_REGION, "us-east-1",
        S3_ACCESS_KEY_ID, "accessKey",
        S3_SECRET_ACCESS_KEY, "secretKey",
        GOOGLE_SERVICE_KEY, "serviceKey"));

    URI s3URI = URI.create("s3://bucket/hello.txt");
    URI googleURI = URI.create("gs://bucket/hello.txt");

    // Mocking construction - just want to make sure that the fetcher is appropriately deferring to each storage client
    // as appropriate based on the strings it is supplied.
    try (
        MockedConstruction<S3StorageClient> mockedConstruction = mockConstruction(S3StorageClient.class, (mock, context) -> {
          when(mock.getFileContentStream(s3URI)).thenReturn(new ByteArrayInputStream("Hello there - S3.".getBytes()));
          when(mock.isInitialized()).thenReturn(true);
        });
        MockedConstruction<GoogleStorageClient> mockedGoogle = mockConstruction(GoogleStorageClient.class, (mock, context) -> {
          when(mock.getFileContentStream(googleURI)).thenReturn(new ByteArrayInputStream("Hello there - Google.".getBytes()));
          when(mock.isInitialized()).thenReturn(true);
        });
    ) {
      FileContentFetcher fetcher = new FileContentFetcher(cloudOptions);
      fetcher.startup();

      URI localPathURI = Paths.get("src/test/resources/FileContentFetcherTest/hello.txt").toUri();
      InputStream localInputStream = fetcher.getInputStream(localPathURI.toString());
      assertEquals("Hello there.", new String(localInputStream.readAllBytes()));

      InputStream s3MockInputStream = fetcher.getInputStream(s3URI.toString());
      assertEquals("Hello there - S3.", new String(s3MockInputStream.readAllBytes()));

      InputStream googleMockInputStream = fetcher.getInputStream(googleURI.toString());
      assertEquals("Hello there - Google.", new String(googleMockInputStream.readAllBytes()));

      assertThrows(IOException.class, () -> fetcher.getInputStream("https://storagename.blob.core.windows.net/bucket/hello.txt"));
    }
  }

  @Test
  public void testClientFailsInit() throws Exception {
    Config cloudOptions = ConfigFactory.parseMap(Map.of(
        S3_REGION, "us-east-1",
        S3_ACCESS_KEY_ID, "accessKey",
        S3_SECRET_ACCESS_KEY, "secretKey"));

    try (MockedConstruction<S3StorageClient> mockedConstruction = mockConstruction(S3StorageClient.class, (mock, context) -> {
      doThrow(new IOException("Mock Init Exception")).when(mock).init();
      doThrow(new IOException("Mock Shutdown Exception")).when(mock).shutdown();
    })) {
      FileContentFetcher fetcher = new FileContentFetcher(cloudOptions);
      assertThrows(IOException.class, () -> fetcher.startup());

      // an error in fetcher.startup() should call shutdown automatically, which will shutdown each storage client.
      S3StorageClient mockS3 = mockedConstruction.constructed().get(0);
      verify(mockS3, times(1)).shutdown();
    }
  }

  @Test
  public void testStaticFetches() throws Exception {
    Config cloudOptions = ConfigFactory.parseMap(Map.of(
        S3_REGION, "us-east-1",
        S3_ACCESS_KEY_ID, "accessKey",
        S3_SECRET_ACCESS_KEY, "secretKey"));

    URI s3URI = URI.create("s3://bucket/hello.txt");

    // Mocking construction - just want to make sure that the fetcher is appropriately deferring to each storage client
    // as appropriate based on the strings it is supplied.
    try (MockedConstruction<S3StorageClient> mockedConstruction = mockConstruction(S3StorageClient.class, (mock, context) -> {
      when(mock.getFileContentStream(s3URI)).thenReturn(new ByteArrayInputStream("Hello there - S3.".getBytes()));
    })) {
      InputStream s3MockInputStream = FileContentFetcher.getOneTimeInputStream(s3URI.toString(), cloudOptions);
      // S3 should've been created once here
      assertEquals(1, mockedConstruction.constructed().size());

      S3StorageClient mockedClient = mockedConstruction.constructed().get(0);
      verify(mockedClient, times(0)).shutdown();

      assertEquals("Hello there - S3.", new String(s3MockInputStream.readAllBytes()));
      s3MockInputStream.close();
      verify(mockedClient, times(1)).shutdown();

      URI localPathURI = Paths.get("src/test/resources/FileContentFetcherTest/hello.txt").toUri();
      InputStream localInputStream = FileContentFetcher.getOneTimeInputStream(localPathURI.toString(), cloudOptions);
      // another S3 client should not have been created.
      assertEquals(1, mockedConstruction.constructed().size());
      assertEquals("Hello there.", new String(localInputStream.readAllBytes()));

      // Google should still be unavailable
      assertThrows(IOException.class, () -> FileContentFetcher.getOneTimeInputStream("gs://bucket/hello.txt", cloudOptions));
    }
  }
}
