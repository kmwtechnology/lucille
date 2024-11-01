package com.kmwllc.lucille.connector.cloudstorageclients;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import org.junit.Test;

public class CloudStorageClientTest {

  @Test
  public void testGetClient() throws URISyntaxException {
    Map<String, Object> cloudOptions = Map.of();
    CloudStorageClient client = CloudStorageClient.getClient(new URI("gs://bucket/"), null, null,
        null, null, cloudOptions);
    assertTrue(client instanceof GoogleStorageClient);
    client = CloudStorageClient.getClient(new URI("s3://bucket/"), null, null, null,
        null, cloudOptions);
    assertTrue(client instanceof S3StorageClient);
    client = CloudStorageClient.getClient(new URI("https://storagename.blob.core.windows.net/testblob"), null,
        null, null, null, cloudOptions);
    assertTrue(client instanceof AzureStorageClient);

    assertThrows(RuntimeException.class, () -> CloudStorageClient.getClient(new URI("unknown://bucket"), null,
        null, null, null, cloudOptions));
  }
}
