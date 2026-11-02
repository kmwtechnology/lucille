package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.CONTENT;
import static com.kmwllc.lucille.connector.FileConnector.FILE_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration smoke test for {@link AzureStorageClient} against a live Azurite (Azure Blob Storage emulator).
 * Unlike {@link AzureStorageClientTest}, which injects a mocked {@code BlobServiceClient} via {@code setServiceClientForTesting(...)} and therefore never builds the real
 * Azure SDK client, this test exercises the real {@code BlobServiceClientBuilder().buildClient()} path. That
 * path wires up azure-core-http-netty -> reactor-netty -> Netty's event loop, which is exactly where a
 * reactor-netty / Netty version split surfaces at runtime as
 * {@code NoClassDefFoundError: io.netty.channel.MultiThreadIoEventLoopGroup}.
 *
 * <p>This is a failsafe {@code *IT} test, so it runs during {@code mvn verify} rather than in the unit-test
 * phase. It connects to an Azurite instance reachable at the connection string in the
 * {@code AZURITE_CONNECTION_STRING} environment variable, or the well-known Azurite dev connection string on
 * {@code localhost:10000} by default. Each test method begins with a fast reachability probe and is skipped
 * (via JUnit {@code Assume}) rather than failed when no Azurite is reachable, so {@code mvn verify} stays green
 * on machines without Docker. CI provides the Azurite service (see {@code .github/workflows/azurite-smoketest.yml}).
 */
public class AzureStorageClientIT {

  // The Azurite development account/key and default blob endpoint. This is a public, fixed emulator
  // credential documented by Microsoft for Azurite; it is not a secret.
  private static final String DEFAULT_AZURITE_CONNECTION_STRING =
      "DefaultEndpointsProtocol=http;"
          + "AccountName=devstoreaccount1;"
          + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
          + "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";

  private static final String CONNECTION_STRING = connectionString();

  private static BlobServiceClient adminClient;
  private static String containerName;

  private static String connectionString() {
    String fromEnv = System.getenv("AZURITE_CONNECTION_STRING");
    return (fromEnv != null && !fromEnv.isBlank()) ? fromEnv : DEFAULT_AZURITE_CONNECTION_STRING;
  }

  /**
   * Parse host and port out of the {@code BlobEndpoint=...} segment of the connection string so we can do a fast
   * reachability probe. Returns null if the endpoint cannot be parsed.
   */
  private static InetSocketAddress blobEndpointAddress(String conn) {
    for (String part : conn.split(";")) {
      String trimmed = part.trim();
      if (trimmed.startsWith("BlobEndpoint=")) {
        try {
          URI uri = URI.create(trimmed.substring("BlobEndpoint=".length()));
          int port = uri.getPort() != -1 ? uri.getPort() : ("https".equals(uri.getScheme()) ? 443 : 80);
          return new InetSocketAddress(uri.getHost(), port);
        } catch (Exception e) {
          return null;
        }
      }
    }
    return null;
  }

  /** Fast check: is something listening on the Azurite blob endpoint? Avoids the Azure SDK's long retry/timeout. */
  private static boolean azuriteReachable(String conn) {
    InetSocketAddress addr = blobEndpointAddress(conn);
    if (addr == null) {
      return false;
    }
    try (Socket socket = new Socket()) {
      socket.connect(addr, 1000);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Per-method guard and lazy fixture setup. The assumption runs inside each test method (via {@code @Before}),
   * so when Azurite is absent JUnit reports each method as <em>skipped</em> (counted) rather than the whole class
   * running zero tests, which failsafe's {@code verify} goal would treat as a failure. When Azurite is present,
   * the shared container and its blobs are created once (idempotently).
   */
  @Before
  public void requireAzuriteAndSeed() {
    assumeTrue("Azurite not reachable at " + CONNECTION_STRING + ". Skipping Azure integration smoke test",
        azuriteReachable(CONNECTION_STRING));

    if (adminClient == null) {
      adminClient = new BlobServiceClientBuilder()
          .connectionString(CONNECTION_STRING)
          .buildClient();

      containerName = "lucille-it-" + UUID.randomUUID();
      BlobContainerClient container = adminClient.createBlobContainer(containerName);
      container.getBlobClient("blob1.txt")
          .upload(toStream("hello from azurite"), "hello from azurite".length(), true);
      container.getBlobClient("blob2.txt")
          .upload(toStream("second blob"), "second blob".length(), true);
    }
  }

  @AfterClass
  public static void tearDown() {
    if (adminClient != null && containerName != null) {
      try {
        adminClient.deleteBlobContainer(containerName);
      } catch (Exception ignored) {
        // best-effort cleanup
      }
    }
  }

  /**
   * End-to-end traversal against Azurite: the real client lists and publishes the blobs in the test container.
   *
   * <p>This is tharegression guard. It must make a real request through the Azure SDK, because
   * the Netty event-loop group (whose missing {@code MultiThreadIoEventLoopGroup} caused the original
   * {@code NoClassDefFoundError}) is created lazily on the first request, not at client construction /
   * {@code init()}.
   */
  @Test
  public void testTraversePublishesBlobs() throws Exception {
    Config cloudOptions = ConfigFactory.parseMap(Map.of("connectionString", CONNECTION_STRING));
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    AzureStorageClient client = new AzureStorageClient(cloudOptions);
    URI containerUri = URI.create("https://devstoreaccount1.blob.core.windows.net/" + containerName + "/");
    TraversalParams params = new TraversalParams(ConfigFactory.empty(), containerUri, "it-");

    try {
      client.init();
      client.traverse(publisher, params);
    } finally {
      client.shutdown();
    }

    List<Document> docs = messenger.getDocsSentForProcessing();
    assertEquals(2, docs.size());
    for (Document doc : docs) {
      assertTrue(doc.getId().startsWith("it-"));
      assertNotNull(doc.getString(FILE_PATH));
      assertNotNull(doc.getBytes(CONTENT));
    }
  }

  private static InputStream toStream(String s) {
    return new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
  }
}
