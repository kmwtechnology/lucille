package com.kmwllc.lucille.connector.storageclient;

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
    Config gCloudOptions = ConfigFactory.load("StorageClientTest/google.conf");
    StorageClient client = StorageClient.create(new URI("gs://bucket/"), gCloudOptions);
    assertTrue(client instanceof GoogleStorageClient);

    Config s3CloudOptions = ConfigFactory.load("StorageClientTest/s3.conf");
    client = StorageClient.create(new URI("s3://bucket/"), s3CloudOptions);
    assertTrue(client instanceof S3StorageClient);

    Config azCloudOptions = ConfigFactory.load("StorageClientTest/azureConnection.conf");
    client = StorageClient.create(new URI("https://storagename.blob.core.windows.net/testblob"), azCloudOptions);
    assertTrue(client instanceof AzureStorageClient);

    azCloudOptions = ConfigFactory.load("StorageClientTest/azureKeyAndName.conf");
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
    Config azCloudOptionsBad = ConfigFactory.load("StorageClientTest/azureBad.conf");
    assertThrows(IllegalArgumentException.class, () -> StorageClient.create(new URI("https://storagename.blob.core.windows.net/testblob"),
        azCloudOptionsBad));

    // test bad g cloud options
    Config gCloudOptionsBad = ConfigFactory.load("StorageClientTest/googleBad.conf");
    assertThrows(IllegalArgumentException.class, () -> StorageClient.create(new URI("gs://bucket/"),
        gCloudOptionsBad));

    // test bad s3 cloud options, only 2 of 3
    Config s3CloudOptionsBad = ConfigFactory.load("StorageClientTest/s3Bad.conf");
    assertThrows(IllegalArgumentException.class, () -> StorageClient.create(new URI("s3://bucket/"),
        s3CloudOptionsBad));

    client.shutdown();
  }

  @Test
  public void testCreateClientsFull() {
    Config fullConfig = ConfigFactory.load("StorageClientTest/fullClients.conf");
    Map<String, StorageClient> results = StorageClient.createClients(fullConfig);

    assertEquals(4, results.size());
  }

  @Test
  public void testCreateClientsEmpty() {
    Map<String, StorageClient> results = StorageClient.createClients(ConfigFactory.empty());

    // ALWAYS get a LocalStorageClient.
    assertEquals(1, results.size());
  }
}
