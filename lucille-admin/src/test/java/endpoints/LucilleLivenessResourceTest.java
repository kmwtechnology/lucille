package endpoints;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.codahale.metrics.health.HealthCheck.Result;
import com.kmwllc.lucille.endpoints.LucilleLivenessResource;
import jakarta.ws.rs.core.Response;
import org.junit.Test;

public class LucilleLivenessResourceTest {

  private static final LucilleLivenessResource liveness = new LucilleLivenessResource();

  @Test
  public void testLivenessEndpoint() {
    Response response = liveness.isAlive();

    assertEquals(Response.ok().build().toString(), response.toString());
  }

}
