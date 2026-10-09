package com.kmwllc.lucille.util;

import com.typesafe.config.Config;
import java.security.GeneralSecurityException;
import javax.net.ssl.SSLContext;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.HostnameVerificationPolicy;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;

/**
 * Builds the httpclient5 connection settings shared by the OpenSearch and Elasticsearch clients.
 *
 * <p> Three optional settings are read from the client's config block (e.g. <code>opensearch</code> or
 * <code>elasticsearch</code>):
 * <ul>
 *   <li>connectTimeoutMs (Long) : maximum time to establish a TCP/TLS connection. Defaults to
 *   {@value #DEFAULT_CONNECT_TIMEOUT_MS}.</li>
 *   <li>socketTimeoutMs (Long) : maximum time to wait for data on an open connection, including the wait for a
 *   response. A request that exceeds it fails with a transport error, which indexers treat as retryable (status -1).
 *   The server gets no signal and may still complete the request. It applies to every request, including synchronous
 *   delete-by-query, so raise it above the slowest expected bulk or delete, or set it to {@value #NO_SOCKET_TIMEOUT}
 *   to wait indefinitely. Defaults to {@value #DEFAULT_SOCKET_TIMEOUT_MS}.</li>
 *   <li>connectionTimeToLiveMs (Long) : maximum lifetime of a pooled connection, after which it is closed instead of
 *   reused. Load balancers, Kubernetes Services and multi-address DNS names balance new connections, not requests, so
 *   this is what lets a long-running client reach nodes added or replaced since it first connected. Expiry is checked
 *   when a connection is next leased, so it never interrupts a request. Defaults to
 *   {@value #DEFAULT_CONNECTION_TIME_TO_LIVE_MS}.</li>
 * </ul>
 *
 * <p> The connect timeout default is the one opensearch-java and elasticsearch-java apply to the connection managers
 * they build themselves, which Lucille replaces with its own. The socket timeout default is opensearch-java's; without
 * one, a stalled request would wait indefinitely.
 */
public class HttpClientConfigUtils {

  public static final long DEFAULT_CONNECT_TIMEOUT_MS = 1000;
  public static final long DEFAULT_CONNECTION_TIME_TO_LIVE_MS = 300_000;
  public static final long DEFAULT_SOCKET_TIMEOUT_MS = 30_000;
  /** The socketTimeoutMs value that disables the socket timeout. */
  public static final long NO_SOCKET_TIMEOUT = -1;

  /** Builds the default {@link ConnectionConfig} for a client from the given config block (e.g. "opensearch"). */
  public static ConnectionConfig buildConnectionConfig(Config config, String configKey) {
    ConnectionConfig.Builder builder = ConnectionConfig.custom()
        .setConnectTimeout(Timeout.ofMilliseconds(getMillis(config, configKey + ".connectTimeoutMs", DEFAULT_CONNECT_TIMEOUT_MS)))
        .setTimeToLive(TimeValue.ofMilliseconds(
            getMillis(config, configKey + ".connectionTimeToLiveMs", DEFAULT_CONNECTION_TIME_TO_LIVE_MS)));

    String socketTimeoutPath = configKey + ".socketTimeoutMs";
    if (config.hasPath(socketTimeoutPath) && config.getLong(socketTimeoutPath) == NO_SOCKET_TIMEOUT) {
      builder.setSocketTimeout(Timeout.DISABLED);
    } else {
      builder.setSocketTimeout(Timeout.ofMilliseconds(getMillis(config, socketTimeoutPath, DEFAULT_SOCKET_TIMEOUT_MS)));
    }
    return builder.build();
  }

  /**
   * Builds the TLS strategy for a client. When acceptInvalidCert is true, any certificate is trusted and the host name
   * is not checked.
   */
  public static TlsStrategy buildTlsStrategy(boolean acceptInvalidCert) throws GeneralSecurityException {
    if (!acceptInvalidCert) {
      SSLContext sslContext = SSLContextBuilder.create().build();
      return ClientTlsStrategyBuilder.create()
          .setSslContext(sslContext)
          .build();
    }

    SSLContext sslContext = SSLContextBuilder.create()
        .loadTrustMaterial(null, (chains, authType) -> true)
        .build();

    // The no-op verifier only takes effect under the CLIENT policy. Under the default (BOTH), httpclient5 5.6.4+ also
    // has JSSE check the host name during the handshake, which rejects a certificate that doesn't match the host.
    return ClientTlsStrategyBuilder.create()
        .setSslContext(sslContext)
        .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
        .setHostVerificationPolicy(HostnameVerificationPolicy.CLIENT)
        .build();
  }

  // getLong, unlike ConfigUtils.getOrDefault, converts a string value such as an environment substitution.
  private static long getMillis(Config config, String path, long fallback) {
    if (!config.hasPath(path)) {
      return fallback;
    }
    long value = config.getLong(path);
    if (value < 1) {
      throw new IllegalArgumentException(path + " must be at least 1.");
    }
    return value;
  }
}
