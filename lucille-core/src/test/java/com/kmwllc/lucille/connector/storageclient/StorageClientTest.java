package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.AZURE_ACCOUNT_KEY;
import static com.kmwllc.lucille.connector.FileConnector.AZURE_ACCOUNT_NAME;
import static com.kmwllc.lucille.connector.FileConnector.AZURE_CONNECTION_STRING;
import static com.kmwllc.lucille.connector.FileConnector.GOOGLE_SERVICE_KEY;
import static com.kmwllc.lucille.connector.FileConnector.S3_ACCESS_KEY_ID;
import static com.kmwllc.lucille.connector.FileConnector.S3_REGION;
import static com.kmwllc.lucille.connector.FileConnector.S3_SECRET_ACCESS_KEY;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.typesafe.config.ConfigFactory;
import java.net.URI;
import java.util.Map;
import org.junit.Test;

public class StorageClientTest {

  @Test
  public void testCreate() throws Exception {
    Map<String, Object> gCloudOptions = Map.of(GOOGLE_SERVICE_KEY, "key");
    StorageClient client = StorageClient.create(new URI("gs://bucket/"), null,
        null, null, gCloudOptions, ConfigFactory.empty());
    assertTrue(client instanceof GoogleStorageClient);

    Map<String, Object> s3CloudOptions = Map.of(S3_REGION, "region", S3_ACCESS_KEY_ID, "id", S3_SECRET_ACCESS_KEY, "key");
    client = StorageClient.create(new URI("s3://bucket/"), null, null,
        null, s3CloudOptions, ConfigFactory.empty());
    assertTrue(client instanceof S3StorageClient);

    Map<String, Object> azCloudOptions = Map.of(AZURE_CONNECTION_STRING, "connectionString");
    client = StorageClient.create(new URI("https://storagename.blob.core.windows.net/testblob"),
        null, null, null, azCloudOptions, ConfigFactory.empty());
    assertTrue(client instanceof AzureStorageClient);

    azCloudOptions = Map.of(AZURE_ACCOUNT_KEY, "key", AZURE_ACCOUNT_NAME, "name");
    client = StorageClient.create(new URI("https://storagename.blob.core.windows.net/testblob"),
        null, null, null, azCloudOptions, ConfigFactory.empty());
    assertTrue(client instanceof AzureStorageClient);

    // test local storage client
    client = StorageClient.create(new URI("file:///path/to/file"), null, null,
        null, Map.of(), ConfigFactory.empty());
    assertTrue(client instanceof LocalStorageClient);

    client = StorageClient.create(new URI("/path/to/file"), null, null,
        null, Map.of(), ConfigFactory.empty());
    assertTrue(client instanceof LocalStorageClient);

    // give wrong uri but correct cloudOptions
    assertThrows(RuntimeException.class, () -> StorageClient.create(new URI("unknown://bucket"),
        null, null, null, gCloudOptions, ConfigFactory.empty()));

    // test bad azure cloud options
    Map<String, Object> azCloudOptionsBad = Map.of(AZURE_ACCOUNT_KEY, "key");
    assertThrows(IllegalArgumentException.class, () ->StorageClient.create(new URI("https://storagename.blob.core.windows.net/testblob"),
        null, null, null, azCloudOptionsBad, ConfigFactory.empty()));

    // test bad g cloud options
    Map<String, Object> gCloudOptionsBad = Map.of();
    assertThrows(IllegalArgumentException.class, () ->StorageClient.create(new URI("gs://bucket/"),
        null, null, null, gCloudOptionsBad, ConfigFactory.empty()));

    // test bad s3 cloud options, only 2 of 3
    Map<String, Object> s3CloudOptionsBad = Map.of(S3_REGION, "region", S3_ACCESS_KEY_ID, "id");
    assertThrows(IllegalArgumentException.class, () ->StorageClient.create(new URI("s3://bucket/"),
        null, null, null, s3CloudOptionsBad, ConfigFactory.empty()));

    client.shutdown();
  }
}
