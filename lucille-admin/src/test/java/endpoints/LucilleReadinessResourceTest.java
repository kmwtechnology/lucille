package endpoints;

import static org.junit.Assert.assertEquals;

import com.kmwllc.lucille.endpoints.LucilleReadinessResource;
import jakarta.ws.rs.core.Response;
import org.junit.Test;

public class LucilleReadinessResourceTest {

  private static final LucilleReadinessResource readiness = new LucilleReadinessResource();

  @Test
  public void testReadinessEndpoint() {
    Response response = readiness.isReady();

    assertEquals(Response.ok().build().toString(), response.toString());
  }

}
