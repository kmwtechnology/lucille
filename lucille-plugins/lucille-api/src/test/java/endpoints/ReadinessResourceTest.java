package endpoints;

import static org.junit.Assert.assertEquals;

import com.kmwllc.lucille.endpoints.ReadinessResource;
import jakarta.ws.rs.core.Response;
import org.junit.Test;

public class ReadinessResourceTest {

  private static final ReadinessResource readiness = new ReadinessResource();

  @Test
  public void testReadinessEndpoint() {
    Response response = readiness.isReady();

    assertEquals(Response.ok().build().toString(), response.toString());
  }

}
