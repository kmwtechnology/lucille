package endpoints;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.codahale.metrics.health.HealthCheck.Result;
import io.dropwizard.auth.PrincipalImpl;
import com.kmwllc.lucille.endpoints.LivenessResource;
import jakarta.ws.rs.core.Response;

import java.util.Map;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

public class LivenessResourceTest {

  private static final LivenessResource liveness = new LivenessResource();

  @Test
  public void testLivenessEndpoint() {
    System.out.println("=======================================================================================================================");
    Optional<PrincipalImpl> user = Optional.of(new PrincipalImpl("test"));
    Response response = liveness.isAlive();  
    
    // Dump relevant response details
    System.out.println("=== Response Dump ===");
    System.out.println("Status: " + response.getStatus());

    // Print headers
    System.out.println("Headers:");
    for (Map.Entry<String, List<Object>> header : response.getHeaders().entrySet()) {
        System.out.println("  " + header.getKey() + ": " + header.getValue());
    }

    // Print body (entity)
    Object entity = response.getEntity();
    System.out.println("Body: " + (entity != null ? entity.toString() : "null"));
    

    assertEquals(Response.ok().build().toString(), response.toString());
  }

}
