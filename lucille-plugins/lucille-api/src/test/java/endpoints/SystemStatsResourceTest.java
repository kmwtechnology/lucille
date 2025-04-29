package endpoints;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
}
