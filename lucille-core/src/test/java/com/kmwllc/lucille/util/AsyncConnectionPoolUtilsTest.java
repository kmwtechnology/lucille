package com.kmwllc.lucille.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.junit.Test;

public class AsyncConnectionPoolUtilsTest {

  private static PoolingAsyncClientConnectionManager build(Map<String, Object> settings) {
    Config config = ConfigFactory.parseMap(settings);
    return AsyncConnectionPoolUtils.buildConnectionManager(config, "opensearch", ClientTlsStrategyBuilder.create().build());
  }

  @Test
  public void testDefaultsMatchHttpClientDefaults() throws Exception {
    try (PoolingAsyncClientConnectionManager expected = PoolingAsyncClientConnectionManagerBuilder.create().build();
        PoolingAsyncClientConnectionManager actual = build(Map.of())) {
      assertEquals(expected.getDefaultMaxPerRoute(), actual.getDefaultMaxPerRoute());
      assertEquals(expected.getMaxTotal(), actual.getMaxTotal());
      assertEquals(AsyncConnectionPoolUtils.DEFAULT_MAX_CONNECTIONS_PER_ROUTE, actual.getDefaultMaxPerRoute());
      assertEquals(AsyncConnectionPoolUtils.DEFAULT_MAX_CONNECTIONS_TOTAL, actual.getMaxTotal());
    }
  }

  @Test
  public void testSmallConcurrencyKeepsDefaults() throws Exception {
    try (PoolingAsyncClientConnectionManager manager = build(Map.of("indexer.maxConcurrentBatches", 4))) {
      assertEquals(5, manager.getDefaultMaxPerRoute());
      assertEquals(25, manager.getMaxTotal());
    }
  }

  @Test
  public void testLargeConcurrencyRaisesPerRoute() throws Exception {
    try (PoolingAsyncClientConnectionManager manager = build(Map.of("indexer.maxConcurrentBatches", 8))) {
      assertEquals(9, manager.getDefaultMaxPerRoute());
      assertEquals(25, manager.getMaxTotal());
    }
    try (PoolingAsyncClientConnectionManager manager = build(Map.of("indexer.maxConcurrentBatches", 32))) {
      assertEquals(33, manager.getDefaultMaxPerRoute());
      assertEquals(33, manager.getMaxTotal());
    }
  }

  @Test
  public void testExplicitSettingsWin() throws Exception {
    try (PoolingAsyncClientConnectionManager manager = build(Map.of("indexer.maxConcurrentBatches", 8,
        "opensearch.maxConnectionsPerRoute", 3, "opensearch.maxConnectionsTotal", 7))) {
      assertEquals(3, manager.getDefaultMaxPerRoute());
      assertEquals(7, manager.getMaxTotal());
    }
  }

  @Test
  public void testStringSettings() throws Exception {
    // Environment substitutions always yield strings.
    Config fromEnv = ConfigFactory.parseString(
        "indexer.maxConcurrentBatches: ${?K}\nopensearch { maxConnectionsPerRoute: ${?ROUTE}, maxConnectionsTotal: ${?TOTAL} }")
        .resolveWith(ConfigFactory.parseMap(Map.of("K", "8", "ROUTE", "12", "TOTAL", "40")));
    try (PoolingAsyncClientConnectionManager manager = AsyncConnectionPoolUtils.buildConnectionManager(fromEnv,
        "opensearch", ClientTlsStrategyBuilder.create().build())) {
      assertEquals(12, manager.getDefaultMaxPerRoute());
      assertEquals(40, manager.getMaxTotal());
    }
    // A string maxConcurrentBatches also feeds the per-route fallback.
    try (PoolingAsyncClientConnectionManager manager = build(Map.of("indexer.maxConcurrentBatches", "32"))) {
      assertEquals(33, manager.getDefaultMaxPerRoute());
      assertEquals(33, manager.getMaxTotal());
    }
  }

  @Test
  public void testRejectsNonPositive() {
    assertThrows(IllegalArgumentException.class, () -> build(Map.of("opensearch.maxConnectionsPerRoute", 0)));
    assertThrows(IllegalArgumentException.class, () -> build(Map.of("opensearch.maxConnectionsTotal", 0)));
  }
}
