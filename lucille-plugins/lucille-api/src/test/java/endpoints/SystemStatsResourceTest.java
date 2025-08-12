package endpoints;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.fasterxml.jackson.databind.JsonNode;
import com.kmwllc.lucille.util.LogUtils;
import org.junit.Test;
import com.kmwllc.lucille.endpoints.SystemStatsResource;
import jakarta.ws.rs.core.Response;
import java.util.Map;

public class SystemStatsResourceTest {

  private static final SystemStatsResource resource = new SystemStatsResource();

  @Test
  public void testSystemStatsEndpoint() {
    Response response = resource.getSystemStats();
    assertEquals(200, response.getStatus());
    Object entity = response.getEntity();
    assertTrue(entity instanceof Map);
    @SuppressWarnings("unchecked")
    Map<?, ?> stats = (Map<?, ?>) entity;
    assertTrue(stats.containsKey("cpu"));
    assertTrue(stats.containsKey("ram"));
    assertTrue(stats.containsKey("jvm"));
    assertTrue(stats.containsKey("storage"));
  }

  @Test
  public void testDropwizardMetricsEndpoint() {
    try {
      SharedMetricRegistries.remove(LogUtils.METRICS_REG);
    } catch (Exception ignored) {}

    MetricRegistry reg = new MetricRegistry();
    reg.counter("test.counter").inc(3);
    SharedMetricRegistries.add(LogUtils.METRICS_REG, reg);

    Response response;
    try {
      response = resource.getDropwizardMetrics();
    } finally {
      try {
        SharedMetricRegistries.remove(LogUtils.METRICS_REG);
      } catch (Exception ignored) {}
    }

    assertEquals(200, response.getStatus());
    JsonNode root = (JsonNode) response.getEntity();
    assertTrue(root.has("counters"));
    assertEquals(3, root.get("counters").get("test.counter").get("count").asInt());
  }
}
