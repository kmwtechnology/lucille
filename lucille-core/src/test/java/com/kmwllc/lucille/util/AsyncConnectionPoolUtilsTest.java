package com.kmwllc.lucille.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import org.junit.Test;

/**
 * Tests how AsyncConnectionPoolUtils sizes the OpenSearch/Elasticsearch HTTP connection pool. The per-route limit must
 * be large enough that concurrent indexer sends (indexer.maxConcurrentBatches) never queue for a connection; explicit
 * config overrides win when present.
 */
public class AsyncConnectionPoolUtilsTest {

  @Test
  public void testPerRouteDefaultsToHttpClientDefaultForLowConcurrency() {
    // With K small, the httpclient5 default (5) is already enough.
    assertEquals(AsyncConnectionPoolUtils.DEFAULT_MAX_CONNECTIONS_PER_ROUTE,
        AsyncConnectionPoolUtils.getMaxConnectionsPerRoute(config(2), "opensearch"));
  }

  @Test
  public void testPerRouteDerivesFromConcurrencyWhenHigher() {
    // With K above the default, the per-route limit is K + 1 (one spare connection for pings and other requests).
    assertEquals(9, AsyncConnectionPoolUtils.getMaxConnectionsPerRoute(config(8), "opensearch"));
  }

  @Test
  public void testPerRouteExplicitOverrideWins() {
    Config config = config(8, Map.of("opensearch.maxConnectionsPerRoute", 50));
    assertEquals(50, AsyncConnectionPoolUtils.getMaxConnectionsPerRoute(config, "opensearch"));
  }

  @Test
  public void testTotalDefaultsToHttpClientDefaultOrPerRoute() {
    // Default total is the httpclient5 default (25) unless the per-route value exceeds it.
    assertEquals(AsyncConnectionPoolUtils.DEFAULT_MAX_CONNECTIONS_TOTAL,
        AsyncConnectionPoolUtils.getMaxConnectionsTotal(config(2), "opensearch", 5));
    assertEquals(30, AsyncConnectionPoolUtils.getMaxConnectionsTotal(config(2), "opensearch", 30));
  }

  @Test
  public void testTotalExplicitOverrideWins() {
    Config config = config(2, Map.of("opensearch.maxConnectionsTotal", 100));
    assertEquals(100, AsyncConnectionPoolUtils.getMaxConnectionsTotal(config, "opensearch", 5));
  }

  @Test
  public void testRejectsNonPositiveValues() {
    Config config = config(2, Map.of("opensearch.maxConnectionsPerRoute", 0));
    assertThrows(IllegalArgumentException.class,
        () -> AsyncConnectionPoolUtils.getMaxConnectionsPerRoute(config, "opensearch"));
  }

  private static Config config(int maxConcurrentBatches) {
    return config(maxConcurrentBatches, Map.of());
  }

  private static Config config(int maxConcurrentBatches, Map<String, Object> extra) {
    Map<String, Object> settings = new java.util.HashMap<>(Map.of("indexer.maxConcurrentBatches", maxConcurrentBatches));
    settings.putAll(extra);
    return ConfigFactory.parseMap(settings);
  }
}
