package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.AZURE_ACCOUNT_KEY;
import static com.kmwllc.lucille.connector.FileConnector.AZURE_ACCOUNT_NAME;
import static com.kmwllc.lucille.connector.FileConnector.AZURE_CONNECTION_STRING;
import static com.kmwllc.lucille.connector.FileConnector.GOOGLE_SERVICE_KEY;
import static com.kmwllc.lucille.connector.FileConnector.S3_ACCESS_KEY_ID;
import static com.kmwllc.lucille.connector.FileConnector.S3_REGION;
import static com.kmwllc.lucille.connector.FileConnector.S3_SECRET_ACCESS_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.net.URI;
import java.util.Map;
import org.junit.Test;

public class StorageClientTest {

  @Test
  public void testCreate() throws Exception {
    Config gCloudOptions = ConfigFactory.parseMap(Map.of(GOOGLE_SERVICE_KEY, "key"));
    StorageClient client = StorageClient.create(new URI("gs://bucket/"), gCloudOptions);
    assertTrue(client instanceof GoogleStorageClient);

    Config s3CloudOptions = ConfigFactory.parseMap(Map.of(S3_REGION, "region", S3_ACCESS_KEY_ID, "id", S3_SECRET_ACCESS_KEY, "key"));
    client = StorageClient.create(new URI("s3://bucket/"), s3CloudOptions);
    assertTrue(client instanceof S3StorageClient);

    Config azCloudOptions = ConfigFactory.parseMap(Map.of(AZURE_CONNECTION_STRING, "connectionString"));
    client = StorageClient.create(new URI("https://storagename.blob.core.windows.net/testblob"), azCloudOptions);
    assertTrue(client instanceof AzureStorageClient);

    azCloudOptions =  ConfigFactory.parseMap(Map.of(AZURE_ACCOUNT_KEY, "key", AZURE_ACCOUNT_NAME, "name"));
    client = StorageClient.create(new URI("https://storagename.blob.core.windows.net/testblob"), azCloudOptions);
    assertTrue(client instanceof AzureStorageClient);

    // test local storage client
    client = StorageClient.create(new URI("file:///path/to/file"), ConfigFactory.empty());
    assertTrue(client instanceof LocalStorageClient);

    client = StorageClient.create(new URI("/path/to/file"), ConfigFactory.empty());
    assertTrue(client instanceof LocalStorageClient);

    // give wrong uri but correct cloudOptions
    assertThrows(RuntimeException.class, () -> StorageClient.create(new URI("unknown://bucket"), gCloudOptions));

    // test bad azure cloud options
    Config azCloudOptionsBad = ConfigFactory.parseMap(Map.of(AZURE_ACCOUNT_KEY, "key"));
    assertThrows(IllegalArgumentException.class, () -> StorageClient.create(new URI("https://storagename.blob.core.windows.net/testblob"),
        azCloudOptionsBad));

    // test bad g cloud options
    Config gCloudOptionsBad = ConfigFactory.empty();
    assertThrows(IllegalArgumentException.class, () ->StorageClient.create(new URI("gs://bucket/"),
        gCloudOptionsBad));

    // test bad s3 cloud options, only 2 of 3
    Config s3CloudOptionsBad = ConfigFactory.parseMap(Map.of(S3_REGION, "region", S3_ACCESS_KEY_ID, "id"));
    assertThrows(IllegalArgumentException.class, () ->StorageClient.create(new URI("s3://bucket/"),
        s3CloudOptionsBad));

    client.shutdown();
  }

  @Test
  public void testCreateClientsFull() {
    Map<String, StorageClient> results = StorageClient.createClients(
        ConfigFactory.parseMap(
            Map.of(
                AZURE_CONNECTION_STRING, "connectionString",
                GOOGLE_SERVICE_KEY, "path/folder",
                S3_SECRET_ACCESS_KEY, "secretKey",
                S3_ACCESS_KEY_ID, "keyID",
                S3_REGION, "us-east-1"
            )));

    assertEquals(4, results.size());
  }

  @Test
  public void testCreateClientsEmpty() {
    Map<String, StorageClient> results = StorageClient.createClients(ConfigFactory.empty());

    // ALWAYS get a LocalStorageClient.
    assertEquals(1, results.size());
  }
}
