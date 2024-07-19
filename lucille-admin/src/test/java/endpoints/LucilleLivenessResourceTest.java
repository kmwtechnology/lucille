package endpoints;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.codahale.metrics.health.HealthCheck.Result;
import com.kmwllc.lucille.endpoints.LucilleLivenessResource;
import io.dropwizard.auth.PrincipalImpl;
import jakarta.ws.rs.core.Response;
import java.util.Optional;
import org.junit.Test;

public class LucilleLivenessResourceTest {

  private static final LucilleLivenessResource liveness = new LucilleLivenessResource();

  @Test
  public void testLivenessEndpoint() {
    Optional<PrincipalImpl> user = Optional.of(new PrincipalImpl("test"));
    Response response = liveness.isAlive(user);

    assertEquals(Response.ok().build().toString(), response.toString());
  }

}
