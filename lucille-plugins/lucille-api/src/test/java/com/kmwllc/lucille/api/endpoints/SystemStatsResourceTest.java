package com.kmwllc.lucille.api.endpoints;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.fasterxml.jackson.databind.JsonNode;
import com.kmwllc.lucille.util.LogUtils;
import java.util.UUID;
import org.junit.Test;
import jakarta.ws.rs.core.Response;

public class SystemStatsResourceTest {

  private static final SystemStatsResource resource = new SystemStatsResource();

  @Test
  public void testDropwizardMetricsEndpoint() {
    // Using UUID for the counter name to prevent transient failures due to race conditions
    MetricRegistry reg = SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG);
    String counterName = "test.counter." + UUID.randomUUID();
    reg.counter(counterName).inc(3);

    Response response;
    try {
      response = resource.getDropwizardMetrics();
    } finally {
      reg.remove(counterName);
    }

    assertEquals(200, response.getStatus());
    JsonNode root = (JsonNode) response.getEntity();
    assertTrue(root.has("counters"));
    assertEquals(3, root.get("counters").get(counterName).get("count").asInt());
  }
}
