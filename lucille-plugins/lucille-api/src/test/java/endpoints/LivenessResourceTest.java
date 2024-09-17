package endpoints;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.endpoints.LivenessResource;
import jakarta.ws.rs.core.Response;
import org.junit.Test;

public class LivenessResourceTest {

  private static final LivenessResource liveness = new LivenessResource();

  @Test
  public void testLivenessEndpoint() {
    Response response = liveness.isAlive();

    assertEquals(Response.ok().build().toString(), response.toString());
  }

}