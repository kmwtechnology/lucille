package com.kmwllc.lucille.connector.storageclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.specialized.BlobInputStream;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.RequestPayer;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

/**
 * Tests that each protected hook on S3StorageClient and AzureStorageClient is called, and changes only what it targets.
 */
public class StorageClientHooksTest {

  private static final Config S3_CONFIG = ConfigFactory.parseMap(Map.of("region", "us-east-1"));
  private static final Config AZURE_CONFIG = ConfigFactory.parseMap(Map.of("connectionString", "connectionString"));
  private static final String S3_ROOT = "s3://bucket/";
  private static final String AZURE_ROOT = "https://account.blob.core.windows.net/container/";
  private static final byte[] CONTENT = "content".getBytes(StandardCharsets.UTF_8);

  private static TraversalParams params(String path, Map<String, Object> connectorConfig) {
    return new TraversalParams(ConfigFactory.parseMap(connectorConfig), URI.create(path), "");
  }

  private static List<Document> traverse(BaseStorageClient client, TraversalParams params) throws Exception {
    TestMessenger messenger = new TestMessenger();
    client.initializeForTesting();
    client.traverse(new PublisherImpl(ConfigFactory.empty(), messenger, "run", "pipeline"), params);
    return messenger.getDocsSentForProcessing();
  }

  private static List<String> paths(List<Document> docs) {
    return docs.stream().map(doc -> doc.getString(StorageClientFields.FILE_PATH)).sorted().collect(Collectors.toList());
  }

  // -- S3 --

  // Lists "file.txt" and, unless flat, the "dir/" prefix holding "dir/nested.txt".
  private static S3Client fakeS3() {
    S3Client s3 = mock(S3Client.class);
    when(s3.listObjectsV2Paginator(any(ListObjectsV2Request.class)))
        .thenAnswer(invocation -> new ListObjectsV2Iterable(s3, invocation.getArgument(0)));
    when(s3.listObjectsV2(any(ListObjectsV2Request.class))).thenAnswer(invocation -> {
      ListObjectsV2Request request = invocation.getArgument(0);
      ListObjectsV2Response.Builder response = ListObjectsV2Response.builder().isTruncated(false);
      if (request.delimiter() == null) {
        return response.contents(s3Object("file.txt"), s3Object("dir/nested.txt")).build();
      }
      if (request.prefix().equals("dir/")) {
        return response.contents(s3Object("dir/nested.txt")).build();
      }
      return response.contents(s3Object("file.txt")).commonPrefixes(CommonPrefix.builder().prefix("dir/").build()).build();
    });
    when(s3.getObjectAsBytes(any(GetObjectRequest.class)))
        .thenAnswer(invocation -> ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), CONTENT));
    when(s3.getObject(any(GetObjectRequest.class))).thenAnswer(invocation -> new ResponseInputStream<>(
        GetObjectResponse.builder().build(), AbortableInputStream.create(new ByteArrayInputStream(CONTENT))));
    return s3;
  }

  private static S3Object s3Object(String key) {
    return S3Object.builder().key(key).lastModified(Instant.EPOCH).size((long) CONTENT.length).build();
  }

  private static <T extends S3StorageClient> T withFake(T client, S3Client s3) {
    client.setS3ClientForTesting(s3);
    return client;
  }

  @Test
  public void testS3ClientBuilderHookRunsAfterConfigAndIsWhatGetsBuilt() throws Exception {
    S3ClientBuilder builder = mock(S3ClientBuilder.class, Answers.RETURNS_SELF);
    S3ClientBuilder returnedByHook = mock(S3ClientBuilder.class, Answers.RETURNS_SELF);
    S3StorageClient client = new S3StorageClient(S3_CONFIG) {
      @Override
      protected S3ClientBuilder configureClientBuilder(S3ClientBuilder given) {
        verify(given).region(Region.US_EAST_1);
        return returnedByHook;
      }
    };

    try (MockedStatic<S3Client> s3Client = mockStatic(S3Client.class)) {
      s3Client.when(S3Client::builder).thenReturn(builder);
      client.start();
    }

    verify(returnedByHook).build();
    verify(builder, never()).build();
  }

  @Test
  public void testS3ListHookAppliesToBothListingModes() throws Exception {
    for (String mode : List.of("flat", "hierarchical")) {
      S3Client s3 = fakeS3();
      Map<String, Object> settings = new HashMap<>(Map.of("region", "us-east-1", "listingMode", mode));
      S3StorageClient client = withFake(new S3StorageClient(ConfigFactory.parseMap(settings)) {
        @Override
        protected ListObjectsV2Request.Builder customizeListRequest(ListObjectsV2Request.Builder builder) {
          return builder.requestPayer(RequestPayer.REQUESTER);
        }
      }, s3);

      List<Document> docs = traverse(client, params(S3_ROOT, Map.of("fileOptions", Map.of("getFileContent", false))));

      assertEquals(List.of(S3_ROOT + "dir/nested.txt", S3_ROOT + "file.txt"), paths(docs));
      ArgumentCaptor<ListObjectsV2Request> requests = ArgumentCaptor.forClass(ListObjectsV2Request.class);
      verify(s3, Mockito.atLeastOnce()).listObjectsV2(requests.capture());
      for (ListObjectsV2Request request : requests.getAllValues()) {
        assertEquals(RequestPayer.REQUESTER, request.requestPayer());
        // what the traversal set is still there
        assertEquals("bucket", request.bucket());
        assertEquals(mode.equals("flat") ? null : "/", request.delimiter());
      }
    }
  }

  // A file's content is read whole, or through a stream with maxSizeBytes, and getFileContentStream reads by URI.
  @Test
  public void testS3GetHookAppliesToEveryRead() throws Exception {
    S3Client s3 = fakeS3();
    S3StorageClient client = withFake(new S3StorageClient(S3_CONFIG) {
      @Override
      protected GetObjectRequest.Builder customizeGetRequest(GetObjectRequest.Builder builder) {
        return builder.versionId("v1");
      }
    }, s3);

    traverse(client, params(S3_ROOT, Map.of()));
    traverse(client, params(S3_ROOT, Map.of("fileOptions", Map.of("maxSizeBytes", 100))));
    client.getFileContentStream(URI.create(S3_ROOT + "byUri.txt")).close();

    ArgumentCaptor<GetObjectRequest> whole = ArgumentCaptor.forClass(GetObjectRequest.class);
    verify(s3, Mockito.atLeastOnce()).getObjectAsBytes(whole.capture());
    ArgumentCaptor<GetObjectRequest> streamed = ArgumentCaptor.forClass(GetObjectRequest.class);
    verify(s3, Mockito.atLeastOnce()).getObject(streamed.capture());

    List<GetObjectRequest> all = new ArrayList<>(whole.getAllValues());
    all.addAll(streamed.getAllValues());
    all.forEach(request -> {
      assertEquals("v1", request.versionId());
      assertEquals("bucket", request.bucket());
    });
    assertTrue(streamed.getAllValues().stream().anyMatch(request -> request.key().equals("byUri.txt")));
    assertTrue(streamed.getAllValues().stream().anyMatch(request -> request.key().equals("file.txt")));
  }

  @Test
  public void testS3HooksLeaveRequestsAloneByDefault() throws Exception {
    S3Client s3 = fakeS3();

    traverse(withFake(new S3StorageClient(S3_CONFIG), s3), params(S3_ROOT, Map.of()));

    ArgumentCaptor<GetObjectRequest> gets = ArgumentCaptor.forClass(GetObjectRequest.class);
    verify(s3, Mockito.atLeastOnce()).getObjectAsBytes(gets.capture());
    gets.getAllValues().forEach(request -> assertNull(request.versionId()));
    ArgumentCaptor<ListObjectsV2Request> lists = ArgumentCaptor.forClass(ListObjectsV2Request.class);
    verify(s3, Mockito.atLeastOnce()).listObjectsV2(lists.capture());
    lists.getAllValues().forEach(request -> assertNull(request.requestPayerAsString()));
  }

  @Test
  public void testS3FileReferenceFactory() throws Exception {
    S3StorageClient client = withFake(new S3StorageClient(S3_CONFIG) {
      @Override
      protected FileReference createFileReference(S3Object s3Obj, TraversalParams params) {
        return new S3FileReference(s3Obj, params) {
          @Override
          public Document asDoc(TraversalParams params) throws IOException {
            Document doc = super.asDoc(params);
            doc.setField("etag", "from-" + s3Obj.key());
            return doc;
          }
        };
      }
    }, fakeS3());

    List<Document> docs = traverse(client, params(S3_ROOT, Map.of("fileOptions", Map.of("getFileContent", false))));

    assertEquals(2, docs.size());
    Document plainDoc = traverse(withFake(new S3StorageClient(S3_CONFIG), fakeS3()),
        params(S3_ROOT, Map.of("fileOptions", Map.of("getFileContent", false)))).get(0);
    for (Document doc : docs) {
      assertEquals("from-" + doc.getString(StorageClientFields.FILE_PATH).substring(S3_ROOT.length()), doc.getString("etag"));
    }
    // the factory only added its field: same ids as without it
    assertTrue(docs.stream().anyMatch(doc -> doc.getId().equals(plainDoc.getId())));
  }

  @Test
  public void testUnbuildableFromTheFactorySkipsOnlyThatObject() throws Exception {
    S3StorageClient client = withFake(new S3StorageClient(S3_CONFIG) {
      @Override
      protected FileReference createFileReference(S3Object s3Obj, TraversalParams params) {
        if (s3Obj.key().equals("file.txt")) {
          throw new IllegalArgumentException("Not this one.");
        }
        return super.createFileReference(s3Obj, params);
      }
    }, fakeS3());

    assertEquals(List.of(S3_ROOT + "dir/nested.txt"), paths(traverse(client, params(S3_ROOT, Map.of("fileOptions", Map.of("getFileContent", false))))));
  }

  @Test
  public void testS3DirectoryCallback() throws Exception {
    Map<String, Object> settings = Map.of("region", "us-east-1", "listingMode", "hierarchical");
    S3StorageClient client = withFake(new S3StorageClient(ConfigFactory.parseMap(settings)) {
      @Override
      protected DirectoryVerdict onDirectory(URI directory, com.kmwllc.lucille.core.Publisher publisher, TraversalParams params) {
        return directory.toString().endsWith("/dir/") ? DirectoryVerdict.SKIP : DirectoryVerdict.DESCEND;
      }
    }, fakeS3());

    assertEquals(List.of(S3_ROOT + "file.txt"), paths(traverse(client, params(S3_ROOT, Map.of("fileOptions", Map.of("getFileContent", false))))));
  }

  @Test
  public void testS3StartingDirectoryAndBucketHooks() throws Exception {
    S3Client s3 = fakeS3();
    S3StorageClient client = withFake(new S3StorageClient(S3_CONFIG) {
      @Override
      protected String getStartingDirectory(TraversalParams params) {
        return "elsewhere/";
      }

      @Override
      protected String getBucketOrContainerName(TraversalParams params) {
        return "other-bucket";
      }
    }, s3);

    traverse(client, params(S3_ROOT, Map.of("fileOptions", Map.of("getFileContent", false))));

    ArgumentCaptor<ListObjectsV2Request> request = ArgumentCaptor.forClass(ListObjectsV2Request.class);
    verify(s3).listObjectsV2(request.capture());
    assertEquals("elsewhere/", request.getValue().prefix());
    assertEquals("other-bucket", request.getValue().bucket());
  }

  // -- The document factory, on the base class --

  @Test
  public void testBuildDocumentHookAppliesToAPlainFile() throws Exception {
    S3StorageClient client = withFake(new S3StorageClient(S3_CONFIG) {
      @Override
      protected Document buildDocument(FileReference fileReference, TraversalParams params) throws IOException {
        Document doc = super.buildDocument(fileReference, params);
        doc.setField("added", true);
        return doc;
      }
    }, fakeS3());

    List<Document> docs = traverse(client, params(S3_ROOT, Map.of("fileOptions", Map.of("getFileContent", false))));

    assertEquals(2, docs.size());
    docs.forEach(doc -> assertTrue(doc.getBoolean("added")));
  }

  @Test
  public void testBuildDocumentIfIncludedPublishesNothingAndAppliesFilters() throws Exception {
    S3Client s3 = fakeS3();
    S3StorageClient client = withFake(new S3StorageClient(S3_CONFIG), s3);
    client.initializeForTesting();
    TraversalParams params = params(S3_ROOT, Map.of("filterOptions", Map.of("excludes", List.of("excluded.*")), "fileOptions", Map.of("moveToAfterProcessing", "s3://other/done/")));

    Document included = client.buildDocumentIfIncluded(client.createFileReference(s3Object("file.txt"), params), params, null);
    Document excluded = client.buildDocumentIfIncluded(client.createFileReference(s3Object("excluded.txt"), params), params, null);
    Document directory = client.buildDocumentIfIncluded(client.createFileReference(s3Object("dir/"), params), params, null);

    assertEquals(S3_ROOT + "file.txt", included.getString(StorageClientFields.FILE_PATH));
    assertEquals("content", new String(included.getBytes(StorageClientFields.CONTENT), StandardCharsets.UTF_8));
    assertNull(excluded);
    assertNull(directory);
    // not a traversal: nothing is moved
    verify(s3, never()).copyObject(any(software.amazon.awssdk.services.s3.model.CopyObjectRequest.class));
  }

  @Test
  public void testBuildDocumentIfIncludedAppliesIncrementalFilter() throws Exception {
    S3StorageClient client = withFake(new S3StorageClient(S3_CONFIG), fakeS3());
    client.initializeForTesting();
    TraversalParams params = params(S3_ROOT, Map.of("filterOptions", Map.of("publishMode", "incremental"), "fileOptions", Map.of("getFileContent", false)));
    FileReference reference = client.createFileReference(s3Object("file.txt"), params);

    assertNull(client.buildDocumentIfIncluded(reference, params, Instant.now()));
    assertEquals(S3_ROOT + "file.txt", client.buildDocumentIfIncluded(reference, params, null).getString(StorageClientFields.FILE_PATH));
  }

  // -- Azure --

  private static BlobItem blob(String name, boolean prefix) {
    OffsetDateTime time = OffsetDateTime.of(2026, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
    BlobItem blob = new BlobItem();
    blob.setName(name);
    if (prefix) {
      blob.setIsPrefix(true);
    }
    blob.setProperties(new BlobItemProperties().setCreationTime(time).setLastModified(time).setContentLength((long) CONTENT.length));
    return blob;
  }

  @SuppressWarnings("unchecked")
  private static PagedIterable<BlobItem> pages(BlobItem... blobs) {
    PagedIterable<BlobItem> iterable = mock(PagedIterable.class);
    when(iterable.stream()).thenAnswer(invocation -> List.of(blobs).stream());
    return iterable;
  }

  // Lists "file.txt" and, unless flat, the "dir/" prefix holding "dir/nested.txt".
  private static BlobContainerClient fakeContainer() {
    BlobContainerClient container = mock(BlobContainerClient.class, RETURNS_DEEP_STUBS);
    when(container.listBlobsByHierarchy(any(), any(), any())).thenAnswer(invocation -> {
      ListBlobsOptions options = invocation.getArgument(1);
      return options.getPrefix().equals("dir/") ? pages(blob("dir/nested.txt", false)) : pages(blob("file.txt", false), blob("dir/", true));
    });
    when(container.listBlobs(any(ListBlobsOptions.class), any())).thenAnswer(invocation -> pages(blob("file.txt", false), blob("dir/nested.txt", false)));
    return container;
  }

  private static <T extends AzureStorageClient> T withFake(T client, BlobContainerClient container) {
    BlobServiceClient service = mock(BlobServiceClient.class);
    when(service.getBlobContainerClient(any())).thenReturn(container);
    client.setServiceClientForTesting(service);
    return client;
  }

  @Test
  public void testAzureClientBuilderHookAppliesToBothBuilders() throws Exception {
    List<Config> configs = List.of(AZURE_CONFIG, ConfigFactory.parseMap(Map.of("accountName", "name", "accountKey", "a2V5")));

    for (Config config : configs) {
      BlobServiceClient built = mock(BlobServiceClient.class);
      BlobServiceClientBuilder returnedByHook = mock(BlobServiceClientBuilder.class);
      when(returnedByHook.buildClient()).thenReturn(built);
      List<BlobServiceClientBuilder> given = new ArrayList<>();

      AzureStorageClient client = new AzureStorageClient(config) {
        @Override
        protected BlobServiceClientBuilder configureClientBuilder(BlobServiceClientBuilder builder) {
          given.add(builder);
          return returnedByHook;
        }
      };

      try (MockedConstruction<BlobServiceClientBuilder> constructed = Mockito.mockConstruction(BlobServiceClientBuilder.class, Mockito.withSettings().defaultAnswer(Answers.RETURNS_SELF))) {
        client.start();
        assertEquals(1, given.size());
        assertSame(constructed.constructed().get(0), given.get(0));
        verify(given.get(0), never()).buildClient();
      }

      verify(returnedByHook).buildClient();
      assertSame(built, client.serviceClient);
    }
  }

  // The credentials are on the builder by the time the hook sees it.
  @Test
  public void testAzureClientBuilderHookRunsAfterConfig() throws Exception {
    AzureStorageClient client = new AzureStorageClient(AZURE_CONFIG) {
      @Override
      protected BlobServiceClientBuilder configureClientBuilder(BlobServiceClientBuilder builder) {
        verify(builder).connectionString("connectionString");
        return builder;
      }
    };

    try (MockedConstruction<BlobServiceClientBuilder> constructed = Mockito.mockConstruction(BlobServiceClientBuilder.class, Mockito.withSettings().defaultAnswer(Answers.RETURNS_SELF))) {
      client.start();
      InOrder order = inOrder(constructed.constructed().get(0));
      order.verify(constructed.constructed().get(0)).connectionString("connectionString");
      order.verify(constructed.constructed().get(0)).buildClient();
    }
  }

  @Test
  public void testAzureListOptionsHookAppliesToBothListingModes() throws Exception {
    for (String mode : List.of("flat", "hierarchical")) {
      BlobContainerClient container = fakeContainer();
      AzureStorageClient client = withFake(new AzureStorageClient(ConfigFactory.parseMap(Map.of("connectionString", "connectionString", "listingMode", mode))) {
        @Override
        protected ListBlobsOptions customizeListOptions(ListBlobsOptions options) {
          return options.setMaxResultsPerPage(7);
        }
      }, container);

      List<Document> docs = traverse(client, params(AZURE_ROOT, Map.of("fileOptions", Map.of("getFileContent", false))));

      assertEquals(List.of(AZURE_ROOT + "dir/nested.txt", AZURE_ROOT + "file.txt"), paths(docs));
      ArgumentCaptor<ListBlobsOptions> options = ArgumentCaptor.forClass(ListBlobsOptions.class);
      if (mode.equals("flat")) {
        verify(container).listBlobs(options.capture(), any());
      } else {
        verify(container, Mockito.atLeastOnce()).listBlobsByHierarchy(any(), options.capture(), any());
      }
      options.getAllValues().forEach(value -> assertEquals(Integer.valueOf(7), value.getMaxResultsPerPage()));
      // what the traversal set is still there
      assertEquals("", options.getAllValues().get(0).getPrefix());
    }
  }

  @Test
  public void testAzureReadHooksApplyToEveryRead() throws Exception {
    List<String> streamed = new ArrayList<>();
    List<String> downloaded = new ArrayList<>();
    BlobContainerClient container = fakeContainer();
    when(container.getBlobClient(anyString())).thenAnswer(invocation -> {
      BlobClient blobClient = mock(BlobClient.class);
      when(blobClient.getBlobName()).thenReturn(invocation.getArgument(0));
      return blobClient;
    });
    AzureStorageClient client = withFake(new AzureStorageClient(AZURE_CONFIG) {
      @Override
      protected java.io.InputStream openBlobStream(BlobClient blobClient) {
        streamed.add(blobClient.getBlobName());
        return new ByteArrayInputStream(CONTENT);
      }

      @Override
      protected byte[] downloadBlobContent(BlobClient blobClient) {
        downloaded.add(blobClient.getBlobName());
        return CONTENT;
      }
    }, container);

    List<Document> whole = traverse(client, params(AZURE_ROOT, Map.of()));
    assertEquals(List.of("file.txt", "dir/nested.txt"), downloaded);
    assertTrue(streamed.isEmpty());
    assertEquals("content", new String(whole.get(0).getBytes(StorageClientFields.CONTENT), StandardCharsets.UTF_8));

    traverse(client, params(AZURE_ROOT, Map.of("fileOptions", Map.of("maxSizeBytes", 100))));
    assertEquals(List.of("file.txt", "dir/nested.txt"), streamed);

    client.getFileContentStream(URI.create(AZURE_ROOT + "byUri.txt")).close();
    assertEquals("byUri.txt", streamed.get(2));
  }

  @Test
  public void testAzureReadsAreUnchangedByDefault() throws Exception {
    BlobClient blobClient = mock(BlobClient.class);
    BlobInputStream stream = mock(BlobInputStream.class);
    when(blobClient.openInputStream()).thenReturn(stream);
    BlobContainerClient container = fakeContainer();
    when(container.getBlobClient(anyString())).thenReturn(blobClient);
    AzureStorageClient client = withFake(new AzureStorageClient(AZURE_CONFIG), container);
    client.initializeForTesting();

    assertSame(stream, client.getFileContentStream(URI.create(AZURE_ROOT + "byUri.txt")));
  }

  @Test
  public void testAzureFileReferenceFactory() throws Exception {
    AzureStorageClient client = withFake(new AzureStorageClient(AZURE_CONFIG) {
      @Override
      protected FileReference createFileReference(BlobItem blobItem, TraversalParams params) {
        return new AzureFileReference(blobItem, params) {
          @Override
          public Document asDoc(TraversalParams params) throws IOException {
            Document doc = super.asDoc(params);
            doc.setField("tier", "hot");
            return doc;
          }
        };
      }
    }, fakeContainer());

    List<Document> docs = traverse(client, params(AZURE_ROOT, Map.of("fileOptions", Map.of("getFileContent", false))));

    assertEquals(List.of(AZURE_ROOT + "dir/nested.txt", AZURE_ROOT + "file.txt"), paths(docs));
    docs.forEach(doc -> assertEquals("hot", doc.getString("tier")));
  }

  @Test
  public void testAzureDirectoryCallback() throws Exception {
    AzureStorageClient client = withFake(new AzureStorageClient(AZURE_CONFIG) {
      @Override
      protected DirectoryVerdict onDirectory(URI directory, com.kmwllc.lucille.core.Publisher publisher, TraversalParams params) {
        return directory.toString().endsWith("/dir/") ? DirectoryVerdict.SKIP : DirectoryVerdict.DESCEND;
      }
    }, fakeContainer());

    assertEquals(List.of(AZURE_ROOT + "file.txt"), paths(traverse(client, params(AZURE_ROOT, Map.of("fileOptions", Map.of("getFileContent", false))))));
  }

  @Test
  public void testAzureStartingDirectoryAndContainerHooks() throws Exception {
    BlobContainerClient container = fakeContainer();
    BlobServiceClient service = mock(BlobServiceClient.class);
    when(service.getBlobContainerClient("other-container")).thenReturn(container);
    AzureStorageClient client = new AzureStorageClient(AZURE_CONFIG) {
      @Override
      protected String getStartingDirectory(TraversalParams params) {
        return "elsewhere/";
      }

      @Override
      protected String getBucketOrContainerName(TraversalParams params) {
        return "other-container";
      }
    };
    client.setServiceClientForTesting(service);

    List<Document> docs = traverse(client, params(AZURE_ROOT, Map.of("fileOptions", Map.of("getFileContent", false))));

    ArgumentCaptor<ListBlobsOptions> options = ArgumentCaptor.forClass(ListBlobsOptions.class);
    verify(container, Mockito.atLeastOnce()).listBlobsByHierarchy(any(), options.capture(), any());
    assertEquals("elsewhere/", options.getAllValues().get(0).getPrefix());
    // a reference's path is built from the same container
    assertTrue(paths(docs).get(0), paths(docs).get(0).contains("/other-container/"));
  }
}
