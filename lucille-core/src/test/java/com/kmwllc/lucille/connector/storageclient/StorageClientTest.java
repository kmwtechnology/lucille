package com.kmwllc.lucille.connector.storageclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.net.URI;
import java.util.List;
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

  @Test
  public void testCreateDefaultClasses() throws Exception {
    // without "class", the default clients are created, not a subclass
    Config fullConfig = ConfigFactory.load("StorageClientTest/fullClients.conf");

    assertEquals(GoogleStorageClient.class, StorageClient.create(new URI("gs://bucket/"), fullConfig).getClass());
    assertEquals(S3StorageClient.class, StorageClient.create(new URI("s3://bucket/"), fullConfig).getClass());
    assertEquals(AzureStorageClient.class,
        StorageClient.create(new URI("https://storagename.blob.core.windows.net/testblob"), fullConfig).getClass());

    Map<String, StorageClient> results = StorageClient.createClients(fullConfig);
    assertEquals(GoogleStorageClient.class, results.get("gs").getClass());
    assertEquals(S3StorageClient.class, results.get("s3").getClass());
    assertEquals(AzureStorageClient.class, results.get("https").getClass());
    assertEquals(LocalStorageClient.class, results.get("file").getClass());
  }

  @Test
  public void testCreateCustomClasses() throws Exception {
    Config config = customClassesConfig();

    assertEquals(CustomGoogleStorageClient.class, StorageClient.create(new URI("gs://bucket/"), config).getClass());
    assertEquals(CustomS3StorageClient.class, StorageClient.create(new URI("s3://bucket/"), config).getClass());
    assertEquals(CustomAzureStorageClient.class,
        StorageClient.create(new URI("https://storagename.blob.core.windows.net/testblob"), config).getClass());
  }

  @Test
  public void testCreateClientsCustomClasses() {
    Map<String, StorageClient> results = StorageClient.createClients(customClassesConfig());

    assertEquals(4, results.size());
    assertEquals(CustomGoogleStorageClient.class, results.get("gs").getClass());
    assertEquals(CustomS3StorageClient.class, results.get("s3").getClass());
    assertEquals(CustomAzureStorageClient.class, results.get("https").getClass());
    assertEquals(LocalStorageClient.class, results.get("file").getClass());
  }

  @Test
  public void testCustomClassReceivesProviderConfig() throws Exception {
    Config config = customClassesConfig().withValue("s3.customSetting", ConfigValueFactory.fromAnyRef("value"));
    CustomS3StorageClient client = (CustomS3StorageClient) StorageClient.create(new URI("s3://bucket/"), config);

    assertEquals("value", client.getConfig().getString("customSetting"));
    assertEquals("us-east-1", client.getConfig().getString("region"));
  }

  @Test
  public void testCustomClassOptionsStillValidated() {
    // the default client's validateOptions still applies to a subclass, and its exception is not wrapped
    Config config = ConfigFactory.load("StorageClientTest/azureBad.conf")
        .withValue("azure.class", ConfigValueFactory.fromAnyRef(CustomAzureStorageClient.class.getName()));

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> StorageClient.create(new URI("https://storagename.blob.core.windows.net/testblob"), config));
    assertTrue(e.getMessage().contains("AzureStorageClient"));
  }

  @Test
  public void testInvalidCustomClasses() {
    Config s3Config = ConfigFactory.load("StorageClientTest/s3.conf");

    // class doesn't exist
    Config missing = s3Config.withValue("s3.class", ConfigValueFactory.fromAnyRef("com.example.DoesNotExist"));
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> StorageClient.create(new URI("s3://bucket/"), missing));
    assertTrue(e.getMessage().contains("com.example.DoesNotExist"));
    assertThrows(IllegalArgumentException.class, () -> StorageClient.createClients(missing));

    // class doesn't extend the default client for the scheme
    Config wrongType = s3Config.withValue("s3.class", ConfigValueFactory.fromAnyRef(CustomAzureStorageClient.class.getName()));
    e = assertThrows(IllegalArgumentException.class, () -> StorageClient.create(new URI("s3://bucket/"), wrongType));
    assertTrue(e.getMessage().contains(S3StorageClient.class.getName()));
    assertThrows(IllegalArgumentException.class, () -> StorageClient.createClients(wrongType));

    // class is abstract
    Config abstractClass = s3Config.withValue("s3.class", ConfigValueFactory.fromAnyRef(AbstractS3StorageClient.class.getName()));
    e = assertThrows(IllegalArgumentException.class, () -> StorageClient.create(new URI("s3://bucket/"), abstractClass));
    assertTrue(e.getMessage().contains("abstract"));

    // class has no (Config) constructor
    Config noConstructor = s3Config.withValue("s3.class", ConfigValueFactory.fromAnyRef(NoConfigConstructorS3StorageClient.class.getName()));
    assertThrows(IllegalArgumentException.class, () -> StorageClient.create(new URI("s3://bucket/"), noConstructor));
    assertThrows(IllegalArgumentException.class, () -> StorageClient.createClients(noConstructor));
  }

  @Test
  public void testAllowedHosts() throws Exception {
    Config azConfig = ConfigFactory.load("StorageClientTest/azureConnection.conf");
    URI customHostURI = new URI("https://storage.example.internal/container/file.txt");
    URI azureURI = new URI("https://storagename.blob.core.windows.net/testblob");

    // a custom host is rejected by default
    assertThrows(IllegalArgumentException.class, () -> StorageClient.create(customHostURI, azConfig));

    // and accepted once it is configured
    Config withCustomHost = azConfig.withValue("azure.allowedHosts",
        ConfigValueFactory.fromIterable(List.of("blob.core.windows.net", "storage.example.internal")));
    assertTrue(StorageClient.create(customHostURI, withCustomHost) instanceof AzureStorageClient);
    assertTrue(StorageClient.create(azureURI, withCustomHost) instanceof AzureStorageClient);
    assertThrows(IllegalArgumentException.class,
        () -> StorageClient.create(new URI("https://other.example.com/container/file.txt"), withCustomHost));

    // allowedHosts replaces the default
    Config onlyCustomHost = azConfig.withValue("azure.allowedHosts", ConfigValueFactory.fromIterable(List.of("storage.example.internal")));
    assertTrue(StorageClient.create(customHostURI, onlyCustomHost) instanceof AzureStorageClient);
    assertThrows(IllegalArgumentException.class, () -> StorageClient.create(azureURI, onlyCustomHost));

    // works with a custom azure class
    Config customClass = withCustomHost.withValue("azure.class", ConfigValueFactory.fromAnyRef(CustomAzureStorageClient.class.getName()));
    assertEquals(CustomAzureStorageClient.class, StorageClient.create(customHostURI, customClass).getClass());
  }

  private static Config customClassesConfig() {
    return ConfigFactory.load("StorageClientTest/fullClients.conf")
        .withValue("gcp.class", ConfigValueFactory.fromAnyRef(CustomGoogleStorageClient.class.getName()))
        .withValue("s3.class", ConfigValueFactory.fromAnyRef(CustomS3StorageClient.class.getName()))
        .withValue("azure.class", ConfigValueFactory.fromAnyRef(CustomAzureStorageClient.class.getName()));
  }

  public static class CustomGoogleStorageClient extends GoogleStorageClient {
    public CustomGoogleStorageClient(Config config) {
      super(config);
    }
  }

  public static class CustomS3StorageClient extends S3StorageClient {
    public CustomS3StorageClient(Config config) {
      super(config);
    }

    public Config getConfig() {
      return config;
    }
  }

  public static class CustomAzureStorageClient extends AzureStorageClient {
    public CustomAzureStorageClient(Config config) {
      super(config);
    }
  }

  public abstract static class AbstractS3StorageClient extends S3StorageClient {
    public AbstractS3StorageClient(Config config) {
      super(config);
    }
  }

  public static class NoConfigConstructorS3StorageClient extends S3StorageClient {
    public NoConfigConstructorS3StorageClient() {
      super(ConfigFactory.empty());
    }
  }
}
