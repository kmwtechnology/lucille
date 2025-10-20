package com.kmwllc.lucille.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.connector.storageclient.GoogleStorageClient;
import com.kmwllc.lucille.connector.storageclient.S3StorageClient;
import com.kmwllc.lucille.connector.storageclient.StorageClient;
import com.kmwllc.lucille.core.FileContentFetcher;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Paths;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

public class FileContentFetcherTest {

  @Test
  public void testGetInputStream() throws Exception {
    DefaultFileContentFetcher fetcher = new DefaultFileContentFetcher(ConfigFactory.empty());
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

    // 5. Trying to get a file after a shutdown
    fetcher.shutdown();
    assertThrows(IOException.class, () -> fetcher.getInputStream("classpath:FileContentFetcherTest/hello.txt"));

    // 6. Trying to get a file after never calling startup
    DefaultFileContentFetcher unstartedFetcher = new DefaultFileContentFetcher(ConfigFactory.empty());
    assertThrows(IOException.class, () -> unstartedFetcher.getInputStream("classpath:FileContentFetcherTest/hello.txt"));
  }

  @Test
  public void testMultipleStorageClients() throws Exception {
    Config cloudOptions = ConfigFactory.parseResourcesAnySyntax("FileContentFetcherTest/s3AndGoogle.conf");

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
      DefaultFileContentFetcher fetcher = new DefaultFileContentFetcher(cloudOptions);
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
    Config cloudOptions = ConfigFactory.parseResourcesAnySyntax("FileContentFetcherTest/s3Only.conf");

    try (MockedConstruction<S3StorageClient> mockedConstruction = mockConstruction(S3StorageClient.class, (mock, context) -> {
      doThrow(new IOException("Mock Init Exception")).when(mock).init();
      doThrow(new IOException("Mock Shutdown Exception")).when(mock).shutdown();
    })) {
      DefaultFileContentFetcher fetcher = new DefaultFileContentFetcher(cloudOptions);
      assertThrows(IOException.class, () -> fetcher.startup());

      // an error in fetcher.startup() should call shutdown automatically, which will shutdown each storage client.
      S3StorageClient mockS3 = mockedConstruction.constructed().get(0);
      verify(mockS3, times(1)).shutdown();
    }
  }

  @Test
  public void testStaticFetches() throws Exception {
    Config cloudOptions = ConfigFactory.parseResourcesAnySyntax("FileContentFetcherTest/s3Only.conf");

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

  @Test
  public void testFailedOneTimeInputStream() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("FileContentFetcherTest/s3Only.conf");

    S3StorageClient mockS3StorageClient = mock(S3StorageClient.class);
    doThrow(new IOException("mock exc")).when(mockS3StorageClient).init();

    try (MockedStatic<StorageClient> mockedStatic = mockStatic(StorageClient.class)) {
      mockedStatic.when(() -> StorageClient.create(any(), any())).thenReturn(mockS3StorageClient);

      // the S3StorageClient throws an exception when it is initialized. The exception should be caught, the FileContentFetcher
      // should be shutdown, and the exception should be thrown.
      assertThrows(IOException.class, () -> FileContentFetcher.getOneTimeInputStream("s3://my_bucket/my_directory/hello.txt", config));

      // the storage client gets shutdown by the file fetcher.
      verify(mockS3StorageClient, times(1)).shutdown();
    }
  }

  @Test
  public void testCustomFetcher() throws IOException {
    Config config = ConfigFactory.parseResourcesAnySyntax("FileContentFetcherTest/customFetcher.conf");

    FileContentFetcher fetcher = FileContentFetcher.create(config);

    try(InputStream io = fetcher.getInputStream("test")) {
      assertEquals("Content of fetched content from custom fetcher should be \"Test.\"", "Test.", IOUtils.toString(io, "UTF-8"));
    }
  }
}
