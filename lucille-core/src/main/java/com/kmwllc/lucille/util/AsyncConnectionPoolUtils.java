package com.kmwllc.lucille.util;

import com.kmwllc.lucille.core.Indexer;
import com.typesafe.config.Config;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;

/**
 * Builds the pooled async HTTP connection manager shared by the OpenSearch and Elasticsearch clients, sized so that
 * concurrent indexer sends (indexer.maxConcurrentBatches) never queue for a connection.
 *
 * <p> Two optional settings are read from the client's config block (e.g. <code>opensearch</code> or
 * <code>elasticsearch</code>):
 * <ul>
 *   <li>maxConnectionsPerRoute (Integer) : defaults to the larger of {@value #DEFAULT_MAX_CONNECTIONS_PER_ROUTE} and
 *   indexer.maxConcurrentBatches + 1, leaving one connection free for pings and other requests.</li>
 *   <li>maxConnectionsTotal (Integer) : defaults to the larger of {@value #DEFAULT_MAX_CONNECTIONS_TOTAL} and
 *   maxConnectionsPerRoute.</li>
 * </ul>
 */
public class AsyncConnectionPoolUtils {

  // The httpclient5 PoolingAsyncClientConnectionManagerBuilder defaults, kept so that unset config behaves as before.
  public static final int DEFAULT_MAX_CONNECTIONS_PER_ROUTE = 5;
  public static final int DEFAULT_MAX_CONNECTIONS_TOTAL = 25;

  /** Builds a connection manager for the given client config block, sized for concurrent sends. */
  public static PoolingAsyncClientConnectionManager buildConnectionManager(Config config, String configKey,
      TlsStrategy tlsStrategy) {
    int perRoute = getMaxConnectionsPerRoute(config, configKey);
    int total = getMaxConnectionsTotal(config, configKey, perRoute);
    return PoolingAsyncClientConnectionManagerBuilder.create()
        .setTlsStrategy(tlsStrategy)
        .setMaxConnPerRoute(perRoute)
        .setMaxConnTotal(total)
        .build();
  }

  static int getMaxConnectionsPerRoute(Config config, String configKey) {
    int fallback = Math.max(DEFAULT_MAX_CONNECTIONS_PER_ROUTE, Indexer.getMaxConcurrentBatches(config) + 1);
    return positive(getInt(config, configKey + ".maxConnectionsPerRoute", fallback),
        configKey + ".maxConnectionsPerRoute");
  }

  static int getMaxConnectionsTotal(Config config, String configKey, int perRoute) {
    int fallback = Math.max(DEFAULT_MAX_CONNECTIONS_TOTAL, perRoute);
    return positive(getInt(config, configKey + ".maxConnectionsTotal", fallback),
        configKey + ".maxConnectionsTotal");
  }

  // getInt, unlike ConfigUtils.getOrDefault, converts a string value such as an environment substitution.
  private static int getInt(Config config, String path, int fallback) {
    return config.hasPath(path) ? config.getInt(path) : fallback;
  }

  private static int positive(int value, String path) {
    if (value < 1) {
      throw new IllegalArgumentException(path + " must be at least 1.");
    }
    return value;
  }
}
