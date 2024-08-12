package auth;

import static org.junit.Assert.assertEquals;

import com.kmwllc.lucille.core.RunnerManager;
import com.kmwllc.lucille.endpoints.LucilleAdminResource;
import io.dropwizard.auth.PrincipalImpl;
import jakarta.ws.rs.core.Response;
import java.util.Optional;
import javax.swing.text.html.Option;
import org.junit.Test;

public class BasicAuthenticatorTest {

  @Test
  public void testAuthSuccess() {
    /**
     * The Authentication should pass when any user is passed to the endpoints
     */

    Optional<PrincipalImpl> user = Optional.of(new PrincipalImpl("test"));
    RunnerManager runnerManager = RunnerManager.getInstance();
    LucilleAdminResource api = new LucilleAdminResource(runnerManager);

    Response status = api.getRunStatus(user);
    assertEquals(200, status.getStatus());
  }

  @Test
  public void testAuthFailure() {
    /**
     * If an empty Optional is passed the Authentication should fail
     */

    Optional<PrincipalImpl> noUser = Optional.empty();
    RunnerManager runnerManager = RunnerManager.getInstance();
    LucilleAdminResource api = new LucilleAdminResource(runnerManager);

    Response status = api.getRunStatus(noUser);
    assertEquals(401, status.getStatus());
  }
}
