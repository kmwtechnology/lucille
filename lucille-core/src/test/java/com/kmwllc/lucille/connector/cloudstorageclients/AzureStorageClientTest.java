package com.kmwllc.lucille.connector.cloudstorageclients;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

public class AzureStorageClientTest {

  @Test
  public void testInit() throws Exception{
    Map<String, Object> cloudOptions = new HashMap<>();
    cloudOptions.put("connectionString", "connectionString");

    AzureStorageClient azureStorageClient = new AzureStorageClient(new URI("https://storagename.blob.core.windows.net/testblob"),
        null, null, null, null, cloudOptions);

    try(MockedConstruction<BlobContainerClientBuilder> builder = Mockito.mockConstruction(BlobContainerClientBuilder.class,(mock,context)-> {
      when(mock.connectionString(anyString())).thenReturn(mock);
      when(mock.containerName(anyString())).thenReturn(mock);
      when(mock.buildClient()).thenReturn(mock(BlobContainerClient.class));
    })) {
      azureStorageClient.init();
      verify(builder.constructed().get(0), times(1)).connectionString("connectionString");
    }

    cloudOptions = new HashMap<>();
    cloudOptions.put("accountName", "accountName");
    cloudOptions.put("accountKey", "accountKey");

    azureStorageClient = new AzureStorageClient(new URI("https://storagename.blob.core.windows.net/testblob"), null,
        null, null, null, cloudOptions);

    try(MockedConstruction<BlobContainerClientBuilder> builder = Mockito.mockConstruction(BlobContainerClientBuilder.class,(mock,context)-> {

      when(mock.credential((StorageSharedKeyCredential) any())).thenReturn(mock);
      when(mock.containerName(anyString())).thenReturn(mock);
      when(mock.buildClient()).thenReturn(mock(BlobContainerClient.class));
    })) {
      azureStorageClient.init();
      verify(builder.constructed().get(0), times(1)).credential(any(StorageSharedKeyCredential.class));
    }
  }

  @Test
  public void testPublishFiles() throws Exception {
    Map<String, Object> cloudOptions = new HashMap<>();
    cloudOptions.put("connectionString", "connectionString");
    cloudOptions.put("maxNumOfPages", 5);
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.parseMap(Map.of());
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    AzureStorageClient azureStorageClient = new AzureStorageClient(new URI("azb://test/"), publisher, null, null,
        null, cloudOptions);
    PagedIterable<BlobItem> pagedIterable = mock(PagedIterable.class);
    ArgumentCaptor<ListBlobsOptions> optionsCaptor = ArgumentCaptor.forClass(ListBlobsOptions.class);
    BlobContainerClient containerClient = mock(BlobContainerClient.class);
    when(containerClient.listBlobsByHierarchy(any(), optionsCaptor.capture(), any())).thenReturn(pagedIterable);

    List<BlobItem> blobItems = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      BlobItem blobItem = mock(BlobItem.class, Mockito.RETURNS_DEEP_STUBS);
      BlobItemProperties properties = mock(BlobItemProperties.class);
      when(blobItem.getProperties()).thenReturn(properties);
      when(properties.getContentLength()).thenReturn((long) i);
      when(properties.getCreationTime()).thenReturn(null);
      when(properties.getLastModified()).thenReturn(null);
      when(properties.getContentMd5()).thenReturn(null);
      when(blobItem.getName()).thenReturn("blob" + i);
      blobItems.add(blobItem);
    }
    when(pagedIterable.iterator()).thenReturn(blobItems.iterator());

    try(MockedConstruction<BlobContainerClientBuilder> builder = Mockito.mockConstruction(BlobContainerClientBuilder.class,(mock,context)-> {
      when(mock.connectionString(anyString())).thenReturn(mock);
      when(mock.containerName(anyString())).thenReturn(mock);
      when(mock.buildClient()).thenReturn(containerClient);
    })) {
      azureStorageClient.init();
      azureStorageClient.publishFiles();

      // verify that we called listBlobs once with maxResultsPerPage set to 5
      verify(containerClient, times(1)).listBlobsByHierarchy(any(), any(), any());
      ListBlobsOptions options = optionsCaptor.getValue();
      assert options.getMaxResultsPerPage() == 5;

      List<Document> documents = messenger.getDocsSentForProcessing();
      for (Document doc : documents) {
        System.out.println(doc); // WIP
      }
    }
  }
}
