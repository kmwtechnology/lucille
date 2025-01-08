package auth;

import static org.junit.Assert.assertEquals;

import java.util.Optional;

import org.junit.Test;

import com.kmwllc.lucille.AuthHandler;
import com.kmwllc.lucille.core.RunnerManager;
import com.kmwllc.lucille.endpoints.LucilleResource;

import io.dropwizard.auth.PrincipalImpl;
import jakarta.ws.rs.core.Response;

public class BasicAuthenticatorTest {
	
	  private final AuthHandler authHandler = new AuthHandler(true);

  @Test
  public void testAuthSuccess() {
    /**
     * The Authentication should pass when any user is passed to the endpoints
     */
    Optional<PrincipalImpl> user = Optional.of(new PrincipalImpl("test"));
    RunnerManager runnerManager = RunnerManager.getInstance();
    LucilleResource api = new LucilleResource(runnerManager, authHandler);
    String runId = "runId";
    // Response status = api.getRunStatus(user, runId);
    // assertEquals(200, status.getStatus());
  }

  @Test
  public void testAuthFailure() {
    /**
     * If an empty Optional is passed the Authentication should fail
     */
    Optional<PrincipalImpl> noUser = Optional.empty();
    RunnerManager runnerManager = RunnerManager.getInstance();
    LucilleResource api = new LucilleResource(runnerManager, authHandler);
    String runId = "runId";
//    Response status = api.getRunStatus(noUser, runId);
//    assertEquals(401, status.getStatus());
  }
}
