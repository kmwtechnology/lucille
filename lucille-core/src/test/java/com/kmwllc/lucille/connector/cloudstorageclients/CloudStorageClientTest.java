package com.kmwllc.lucille.connector.cloudstorageclients;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import org.junit.Test;

public class CloudStorageClientTest {

  @Test
  public void testGetClient() throws URISyntaxException {
    CloudStorageClient client = CloudStorageClient.getClient(new URI("gcs://bucket/"), null, null, null, null, null);
    assertTrue(client instanceof GoogleStorageClient);
    client = CloudStorageClient.getClient(new URI("s3://bucket/"), null, null, null, null, null);
    assertTrue(client instanceof S3StorageClient);
    client = CloudStorageClient.getClient(new URI("azb://bucket"), null, null, null, null, null);
    assertTrue(client instanceof AzureStorageClient);

    assertThrows(RuntimeException.class, () -> CloudStorageClient.getClient(new URI("unknown://bucket"), null, null, null, null, null));
  }
}
