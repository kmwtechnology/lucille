package com.kmwllc.lucille.util;

import com.kmwllc.lucille.core.Indexer;
import com.typesafe.config.Config;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;

/**
 * Builds the pooled async HTTP connection manager shared by the OpenSearch and Elasticsearch clients, sized from
 * indexer.maxConcurrentBatches so that concurrent indexer sends never queue for a connection: the larger of
 * {@value #DEFAULT_MAX_CONNECTIONS_PER_ROUTE} and maxConcurrentBatches + 1 per route (leaving a connection free for pings
 * and other requests), and the larger of {@value #DEFAULT_MAX_CONNECTIONS_TOTAL} and that in total. Up to
 * maxConcurrentBatches 4, these are the httpclient5 defaults.
 */
public class AsyncConnectionPoolUtils {

  // The httpclient5 PoolingAsyncClientConnectionManagerBuilder defaults.
  public static final int DEFAULT_MAX_CONNECTIONS_PER_ROUTE = 5;
  public static final int DEFAULT_MAX_CONNECTIONS_TOTAL = 25;

  public static PoolingAsyncClientConnectionManager buildConnectionManager(Config config, TlsStrategy tlsStrategy) {
    int perRoute = Math.max(DEFAULT_MAX_CONNECTIONS_PER_ROUTE, Indexer.getMaxConcurrentBatches(config) + 1);
    return PoolingAsyncClientConnectionManagerBuilder.create()
        .setTlsStrategy(tlsStrategy)
        .setMaxConnPerRoute(perRoute)
        .setMaxConnTotal(Math.max(DEFAULT_MAX_CONNECTIONS_TOTAL, perRoute))
        .build();
  }
}
