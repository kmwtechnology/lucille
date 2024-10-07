package endpoints;

import static org.junit.Assert.assertEquals;

import com.kmwllc.lucille.endpoints.ReadinessResource;
import io.dropwizard.auth.PrincipalImpl;
import jakarta.ws.rs.core.Response;
import java.util.Optional;
import org.junit.Test;

public class ReadinessResourceTest {

  private static final ReadinessResource readiness = new ReadinessResource();

  @Test
  public void testReadinessEndpoint() {
    Optional<PrincipalImpl> user = Optional.of(new PrincipalImpl("test"));
    Response response = readiness.isReady(user);

    assertEquals(Response.ok().build().toString(), response.toString());
  }

}
