package endpoints;

import static org.junit.Assert.assertEquals;
import java.util.Optional;
import org.junit.Test;
import com.kmwllc.lucille.endpoints.ReadinessResource;
import io.dropwizard.auth.PrincipalImpl;
import jakarta.ws.rs.core.Response;

public class ReadinessResourceTest {

  private static final ReadinessResource readiness = new ReadinessResource();

  @Test
  public void testReadinessEndpoint() {
    Optional<PrincipalImpl> user = Optional.of(new PrincipalImpl("test"));
    Response response = readiness.isReady();
    assertEquals(Response.ok().build().toString(), response.toString());
  }

}
