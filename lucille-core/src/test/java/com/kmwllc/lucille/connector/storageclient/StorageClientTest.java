package com.kmwllc.lucille.connector.storageclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
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

    client.stop();
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
  public void testCustomClassReadsOwnSetting() throws Exception {
    // a subclass of a built-in client reads both the built-in settings and its own from its storageClients entry
    Config config = customClassesConfig().withValue("storageClients.s3.endpointOverride", ConfigValueFactory.fromAnyRef("https://s3.example.internal"));
    CustomS3StorageClient client = (CustomS3StorageClient) StorageClient.create(new URI("s3://bucket/"), config);

    assertEquals("https://s3.example.internal", client.getConfig().getString("endpointOverride"));
    assertEquals("us-east-1", client.getConfig().getString("region"));

    client = (CustomS3StorageClient) StorageClient.createClients(config).get("s3");
    assertEquals("https://s3.example.internal", client.getConfig().getString("endpointOverride"));
  }

  @Test
  public void testCustomClassOptionsStillValidated() {
    // the built-in client's validateOptions still applies to a subclass, and its exception is not wrapped
    Config config = ConfigFactory.parseString("storageClients.https { class: \"" + CustomAzureStorageClient.class.getName()
        + "\", accountKey: abc123 }");

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> StorageClient.create(new URI("https://storagename.blob.core.windows.net/testblob"), config));
    assertTrue(e.getMessage().contains("AzureStorageClient"));
  }

  @Test
  public void testBuiltInSchemeInBothBlocks() {
    // a scheme in storageClients can't also have its built-in block
    Map<String, String> builtInBlocks = Map.of("s3", "s3", "gs", "gcp", "https", "azure");
    Config fullConfig = ConfigFactory.load("StorageClientTest/fullClients.conf");

    for (Map.Entry<String, String> entry : builtInBlocks.entrySet()) {
      Config both = memoryClientConfig(entry.getKey()).withFallback(fullConfig);

      IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> StorageClient.createClients(both));
      assertTrue(e.getMessage().contains("storageClients." + entry.getKey() + " and " + entry.getValue()));
      assertThrows(IllegalArgumentException.class, () -> StorageClient.create(new URI("/path/to/file"), both));
    }
  }

  @Test
  public void testInvalidCustomClasses() {
    Config s3Config = customClassesConfig();

    // class doesn't exist
    Config missing = s3Config.withValue("storageClients.s3.class", ConfigValueFactory.fromAnyRef("com.example.DoesNotExist"));
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> StorageClient.create(new URI("s3://bucket/"), missing));
    assertTrue(e.getMessage().contains("com.example.DoesNotExist"));
    assertThrows(IllegalArgumentException.class, () -> StorageClient.createClients(missing));

    // class doesn't implement StorageClient
    Config wrongType = s3Config.withValue("storageClients.s3.class", ConfigValueFactory.fromAnyRef(String.class.getName()));
    e = assertThrows(IllegalArgumentException.class, () -> StorageClient.create(new URI("s3://bucket/"), wrongType));
    assertTrue(e.getMessage().contains("must implement " + StorageClient.class.getName()));
    assertThrows(IllegalArgumentException.class, () -> StorageClient.createClients(wrongType));

    // class is abstract
    Config abstractClass = s3Config.withValue("storageClients.s3.class", ConfigValueFactory.fromAnyRef(AbstractS3StorageClient.class.getName()));
    e = assertThrows(IllegalArgumentException.class, () -> StorageClient.create(new URI("s3://bucket/"), abstractClass));
    assertTrue(e.getMessage().contains("abstract"));

    // class has no (Config) constructor
    Config noConstructor = s3Config.withValue("storageClients.s3.class",
        ConfigValueFactory.fromAnyRef(NoConfigConstructorS3StorageClient.class.getName()));
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
  }

  @Test
  public void testClientFor() throws Exception {
    Config config = ConfigFactory.load("StorageClientTest/fullClients.conf")
        .withValue("azure.allowedHosts", ConfigValueFactory.fromIterable(List.of("storage.example.internal")))
        .withFallback(memoryClientConfig("mem"));
    Map<String, StorageClient> clients = StorageClient.createClients(config);

    assertEquals(clients.get("s3"), StorageClient.clientFor(new URI("s3://bucket/key"), clients, config));
    assertEquals(clients.get("gs"), StorageClient.clientFor(new URI("gs://bucket/key"), clients, config));
    assertEquals(clients.get("mem"), StorageClient.clientFor(new URI("mem://host/key"), clients, config));
    assertEquals(clients.get("file"), StorageClient.clientFor(new URI("/path/to/file"), clients, config));
    assertEquals(clients.get("https"), StorageClient.clientFor(new URI("https://storage.example.internal/c/key"), clients, config));

    // the built-in azure client only handles https URIs on its allowed hosts
    assertNull(StorageClient.clientFor(new URI("https://storagename.blob.core.windows.net/c/key"), clients, config));
    assertNull(StorageClient.clientFor(new URI("https://example.com/c/key"), clients, config));

    // no client for the scheme
    assertNull(StorageClient.clientFor(new URI("other://host/key"), clients, config));

    // a custom https client handles every https URI
    Config customHttps = memoryClientConfig("https");
    Map<String, StorageClient> customClients = StorageClient.createClients(customHttps);
    assertEquals(customClients.get("https"), StorageClient.clientFor(new URI("https://example.com/c/key"), customClients, customHttps));
    assertTrue(StorageClient.create(new URI("https://example.com/c/key"), customHttps) instanceof MemoryStorageClient);
  }

  @Test
  public void testCustomSchemeClient() throws Exception {
    Config config = memoryClientConfig("mem");

    MemoryStorageClient client = (MemoryStorageClient) StorageClient.create(new URI("mem://host/path/file.txt"), config);
    assertEquals("hello", client.getConfig().getString("content"));

    Map<String, StorageClient> results = StorageClient.createClients(config);
    assertEquals(2, results.size());
    assertEquals(MemoryStorageClient.class, results.get("mem").getClass());
    assertEquals(LocalStorageClient.class, results.get("file").getClass());

    // custom clients sit alongside the built-in ones
    Map<String, StorageClient> withCloud = StorageClient.createClients(ConfigFactory.load("StorageClientTest/fullClients.conf").withFallback(config));
    assertEquals(5, withCloud.size());
    assertEquals(S3StorageClient.class, withCloud.get("s3").getClass());
    assertEquals(MemoryStorageClient.class, withCloud.get("mem").getClass());

    // other schemes are still unsupported
    assertThrows(IllegalArgumentException.class, () -> StorageClient.create(new URI("other://host/file.txt"), config));
  }

  @Test
  public void testCustomSchemeClientOptionsValidated() {
    // the client's own validateOptions applies
    Config config = ConfigFactory.parseString("storageClients.mem.class: \"" + MemoryStorageClient.class.getName() + "\"");

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> StorageClient.createClients(config));
    assertTrue(e.getMessage().contains("MemoryStorageClient requires 'content'"));
  }

  @Test
  public void testInvalidCustomSchemeClients() {
    // file can't be configured
    Config fileScheme = memoryClientConfig("file");
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> StorageClient.createClients(fileScheme));
    assertTrue(e.getMessage().contains("file scheme"));
    assertThrows(IllegalArgumentException.class, () -> StorageClient.create(new URI("/path/to/file"), fileScheme));

    // built-in cloud schemes can, replacing the built-in client
    for (String scheme : List.of("s3", "gs", "https")) {
      Map<String, StorageClient> results = StorageClient.createClients(memoryClientConfig(scheme));
      assertEquals(MemoryStorageClient.class, results.get(scheme).getClass());
    }

    // single-letter schemes are Windows drive letters
    assertThrows(IllegalArgumentException.class, () -> StorageClient.createClients(memoryClientConfig("c")));

    // class is required
    Config noClass = ConfigFactory.parseString("storageClients.mem.content: hello");
    e = assertThrows(IllegalArgumentException.class, () -> StorageClient.createClients(noClass));
    assertTrue(e.getMessage().contains("storageClients.mem"));

    // each entry must be a map
    Config notMap = ConfigFactory.parseString("storageClients.mem: hello");
    assertThrows(IllegalArgumentException.class, () -> StorageClient.createClients(notMap));

    // class must implement StorageClient
    Config notStorageClient = memoryClientConfig("mem").withValue("storageClients.mem.class", ConfigValueFactory.fromAnyRef(String.class.getName()));
    e = assertThrows(IllegalArgumentException.class, () -> StorageClient.createClients(notStorageClient));
    assertTrue(e.getMessage().contains("must implement " + StorageClient.class.getName()));
  }

  /**
   * Returns a config registering a MemoryStorageClient for the given scheme in storageClients, with content "hello".
   */
  public static Config memoryClientConfig(String scheme) {
    return ConfigFactory.empty()
        .withValue("storageClients." + scheme + ".class", ConfigValueFactory.fromAnyRef(MemoryStorageClient.class.getName()))
        .withValue("storageClients." + scheme + ".content", ConfigValueFactory.fromAnyRef("hello"));
  }

  /**
   * Returns a config replacing the built-in s3, gs, and https clients with the Custom* subclasses below, through storageClients.
   */
  public static Config customClassesConfig() {
    Config fullConfig = ConfigFactory.load("StorageClientTest/fullClients.conf");
    return ConfigFactory.empty()
        .withValue("storageClients.gs", fullConfig.getValue("gcp"))
        .withValue("storageClients.gs.class", ConfigValueFactory.fromAnyRef(CustomGoogleStorageClient.class.getName()))
        .withValue("storageClients.s3", fullConfig.getValue("s3"))
        .withValue("storageClients.s3.class", ConfigValueFactory.fromAnyRef(CustomS3StorageClient.class.getName()))
        .withValue("storageClients.https", fullConfig.getValue("azure"))
        .withValue("storageClients.https.class", ConfigValueFactory.fromAnyRef(CustomAzureStorageClient.class.getName()));
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
