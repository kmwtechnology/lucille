package com.kmwllc.lucille.connector.storageclients;

import static com.kmwllc.lucille.connector.FileConnector.AZURE_ACCOUNT_KEY;
import static com.kmwllc.lucille.connector.FileConnector.AZURE_ACCOUNT_NAME;
import static com.kmwllc.lucille.connector.FileConnector.AZURE_CONNECTION_STRING;
import static com.kmwllc.lucille.connector.FileConnector.GOOGLE_SERVICE_KEY;
import static com.kmwllc.lucille.connector.FileConnector.S3_ACCESS_KEY_ID;
import static com.kmwllc.lucille.connector.FileConnector.S3_REGION;
import static com.kmwllc.lucille.connector.FileConnector.S3_SECRET_ACCESS_KEY;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.mockito.MockedStatic;

public class StorageClientTest {

  @Test
  public void testGetClient() throws Exception {
    Map<String, Object> gCloudOptions = Map.of(GOOGLE_SERVICE_KEY, "key");
    StorageClient client = StorageClient.getClient(new URI("gs://bucket/"), null, null,
        null, null, gCloudOptions, ConfigFactory.empty());
    assertTrue(client instanceof GoogleStorageClient);

    Map<String, Object> s3CloudOptions = Map.of(S3_REGION, "region", S3_ACCESS_KEY_ID, "id", S3_SECRET_ACCESS_KEY, "key");
    client = StorageClient.getClient(new URI("s3://bucket/"), null, null, null,
        null, s3CloudOptions, ConfigFactory.empty());
    assertTrue(client instanceof S3StorageClient);

    Map<String, Object> azCloudOptions = Map.of(AZURE_CONNECTION_STRING, "connectionString");
    client = StorageClient.getClient(new URI("https://storagename.blob.core.windows.net/testblob"), null,
        null, null, null, azCloudOptions, ConfigFactory.empty());
    assertTrue(client instanceof AzureStorageClient);

    azCloudOptions = Map.of(AZURE_ACCOUNT_KEY, "key", AZURE_ACCOUNT_NAME, "name");
    client = StorageClient.getClient(new URI("https://storagename.blob.core.windows.net/testblob"), null,
        null, null, null, azCloudOptions, ConfigFactory.empty());
    assertTrue(client instanceof AzureStorageClient);

    // give wrong uri but correct cloudOptions
    assertThrows(RuntimeException.class, () -> StorageClient.getClient(new URI("unknown://bucket"), null,
        null, null, null, gCloudOptions, ConfigFactory.empty()));

    // test bad azure cloud options
    Map<String, Object> azCloudOptionsBad = Map.of(AZURE_ACCOUNT_KEY, "key");
    assertThrows(IllegalArgumentException.class, () ->StorageClient.getClient(new URI("https://storagename.blob.core.windows.net/testblob"), null,
        null, null, null, azCloudOptionsBad, ConfigFactory.empty()));

    // test bad g cloud options
    Map<String, Object> gCloudOptionsBad = Map.of();
    assertThrows(IllegalArgumentException.class, () ->StorageClient.getClient(new URI("gs://bucket/"), null,
        null, null, null, gCloudOptionsBad, ConfigFactory.empty()));

    // test bad s3 cloud options, only 2 of 3
    Map<String, Object> s3CloudOptionsBad = Map.of(S3_REGION, "region", S3_ACCESS_KEY_ID, "id");
    assertThrows(IllegalArgumentException.class, () ->StorageClient.getClient(new URI("s3://bucket/"), null,
        null, null, null, s3CloudOptionsBad, ConfigFactory.empty()));

    client.shutdown();
  }
}
