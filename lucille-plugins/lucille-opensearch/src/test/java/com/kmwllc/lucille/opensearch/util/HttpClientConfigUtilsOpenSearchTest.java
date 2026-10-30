package com.kmwllc.lucille.opensearch.util;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import org.junit.After;
import org.junit.Test;
import org.opensearch.client.opensearch.OpenSearchClient;

/**
 * OpenSearch-client end-to-end coverage for the shared {@code com.kmwllc.lucille.util.HttpClientConfigUtils} TLS and
 * socket-timeout behavior.
 */
public class HttpClientConfigUtilsOpenSearchTest {

  private static final String KEYSTORE = "HttpClientConfigUtilsTest/mismatched-host.p12";

  private HttpServer server;
  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private final ExecutorService serverExecutor = Executors.newCachedThreadPool();
  private final List<AutoCloseable> transports = new ArrayList<>();

  @After
  public void tearDown() throws Exception {
    for (AutoCloseable transport : transports) {
      transport.close();
    }
    if (server != null) {
      server.stop(0);
    }
    executor.shutdownNow();
    serverExecutor.shutdownNow();
  }

  private OpenSearchClient openSearchClient(Map<String, Object> settings) throws Exception {
    OpenSearchClient client = OpenSearchUtils.getOpenSearchRestClient(ConfigFactory.parseMap(settings));
    transports.add(client._transport()::close);
    return client;
  }

  // httpclient5 5.6.4 made JSSE check the host name under the default verification policy, bypassing the no-op verifier.
  @Test
  public void testOpenSearchAcceptInvalidCertWithMismatchedHost() throws Exception {
    int port = startHttpsServer();
    OpenSearchClient client = openSearchClient(Map.of(
        "opensearch.url", "https://localhost:" + port,
        "opensearch.acceptInvalidCert", true));

    assertTrue(client.ping().value());
  }

  // Only shows the default path still validates the certificate (this one is self-signed, so trust fails first).
  @Test
  public void testOpenSearchUntrustedCertRejectedByDefault() throws Exception {
    int port = startHttpsServer();
    OpenSearchClient client = openSearchClient(Map.of(
        "opensearch.url", "https://localhost:" + port));

    assertThrows(IOException.class, client::ping);
  }

  @Test
  public void testOpenSearchSocketTimeoutBoundsStalledRequest() throws Exception {
    int port = startStallingServer();
    OpenSearchClient client = openSearchClient(Map.of(
        "opensearch.url", "http://localhost:" + port,
        "opensearch.socketTimeoutMs", 300));

    Future<?> ping = executor.submit(() -> client.ping());
    assertTimesOut(ping);
  }

  // The request must fail with a transport error well before the server's 10 s stall ends.
  private static void assertTimesOut(Future<?> request) throws Exception {
    java.util.concurrent.ExecutionException e = assertThrows(java.util.concurrent.ExecutionException.class,
        () -> request.get(5, TimeUnit.SECONDS));
    assertTrue("expected an IOException, got " + e.getCause(), e.getCause() instanceof IOException);
  }

  private int startStallingServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/", exchange -> {
      try {
        Thread.sleep(10_000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      exchange.close();
    });
    server.setExecutor(serverExecutor);
    server.start();
    return server.getAddress().getPort();
  }

  // Serves a certificate issued to not-this-host.example, so a client connecting to localhost sees a host mismatch.
  private int startHttpsServer() throws Exception {
    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    try (InputStream in = getClass().getClassLoader().getResourceAsStream(KEYSTORE)) {
      keyStore.load(in, "changeit".toCharArray());
    }
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(keyStore, "changeit".toCharArray());
    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(kmf.getKeyManagers(), null, null);

    HttpsServer httpsServer = HttpsServer.create(new InetSocketAddress("localhost", 0), 0);
    httpsServer.setHttpsConfigurator(new HttpsConfigurator(sslContext));
    httpsServer.createContext("/", exchange -> {
      exchange.getResponseHeaders().set("X-Elastic-Product", "Elasticsearch");
      exchange.sendResponseHeaders(200, -1);
      exchange.close();
    });
    httpsServer.start();
    server = httpsServer;
    return httpsServer.getAddress().getPort();
  }
}
