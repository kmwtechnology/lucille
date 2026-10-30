package com.kmwllc.lucille.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.junit.Test;

public class HttpClientConfigUtilsTest {

  @Test
  public void testDefaults() {
    ConnectionConfig connectionConfig = HttpClientConfigUtils.buildConnectionConfig(ConfigFactory.empty(), "opensearch");

    assertEquals(Timeout.ofMilliseconds(HttpClientConfigUtils.DEFAULT_CONNECT_TIMEOUT_MS), connectionConfig.getConnectTimeout());
    assertEquals(Timeout.ofMilliseconds(30_000), connectionConfig.getSocketTimeout());
    assertEquals(TimeValue.ofMilliseconds(300_000), connectionConfig.getTimeToLive());
  }

  @Test
  public void testSocketTimeoutMinusOneDisablesIt() {
    Config config = ConfigFactory.parseMap(Map.of("opensearch.socketTimeoutMs", -1));

    assertEquals(Timeout.DISABLED, HttpClientConfigUtils.buildConnectionConfig(config, "opensearch").getSocketTimeout());
  }

  @Test
  public void testConfiguredValues() {
    Config config = ConfigFactory.parseMap(Map.of(
        "elasticsearch.connectTimeoutMs", 10000,
        "elasticsearch.socketTimeoutMs", "120000",
        "elasticsearch.connectionTimeToLiveMs", 60000));

    ConnectionConfig connectionConfig = HttpClientConfigUtils.buildConnectionConfig(config, "elasticsearch");

    assertEquals(Timeout.ofMilliseconds(10000), connectionConfig.getConnectTimeout());
    assertEquals(Timeout.ofMilliseconds(120000), connectionConfig.getSocketTimeout());
    assertEquals(TimeValue.ofMilliseconds(60000), connectionConfig.getTimeToLive());
  }

  @Test
  public void testRejectsNonPositiveValues() {
    Map<String, int[]> invalid = Map.of(
        "connectTimeoutMs", new int[] {0, -1},
        "connectionTimeToLiveMs", new int[] {0, -1},
        // -1 is allowed for the socket timeout, where it disables the timeout
        "socketTimeoutMs", new int[] {0, -2});
    invalid.forEach((key, values) -> {
      for (int value : values) {
        Config config = ConfigFactory.parseMap(Map.of("opensearch." + key, value));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
            () -> HttpClientConfigUtils.buildConnectionConfig(config, "opensearch"));
        assertTrue(e.getMessage().contains("opensearch." + key));
      }
    });
  }
}
