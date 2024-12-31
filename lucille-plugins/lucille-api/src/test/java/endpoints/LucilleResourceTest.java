package endpoints;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import com.kmwllc.lucille.AuthHandler;
import com.kmwllc.lucille.core.RunnerManager;
import com.kmwllc.lucille.endpoints.LucilleResource;

import io.dropwizard.auth.PrincipalImpl;
import jakarta.ws.rs.core.Response;

@RunWith(MockitoJUnitRunner.class)
public class LucilleResourceTest {

  
  private final RunnerManager runnerManager = RunnerManager.getInstance();
  private final Optional<PrincipalImpl> user = Optional.of(new PrincipalImpl("test"));
  private final String runId = "runId";
  private final AuthHandler authHandler = new AuthHandler(true);

  @Test
  public void testGetRunStatus() throws Exception {
    runnerManager.waitForRunCompletion(runId);

    LucilleResource admin = new LucilleResource(runnerManager, authHandler);
    Response status = admin.getRunStatus(user, runId);

    assertEquals("{'isRunning': 'false', 'runId': ''}", status.getEntity().toString());
  }

  @Test
  public void testRun() throws Exception {
    runnerManager.waitForRunCompletion(runId);

    LucilleResource admin = new LucilleResource(runnerManager, authHandler);

    Response status = Response.status(404).build();
    try {
      Map<String,Object> config = new HashMap<>();
      config.put("pipeline", "data");
      
      status = admin.startRun(user, config);
    } catch (Exception e) {

    } finally {
      assertEquals(200, status.getStatus());
    }
  }
}
