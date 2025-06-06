package endpoints;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import com.kmwllc.lucille.AuthHandler;
import com.kmwllc.lucille.core.RunDetails;
import com.kmwllc.lucille.core.RunnerManager;
import com.kmwllc.lucille.endpoints.LucilleResource;
import io.dropwizard.auth.PrincipalImpl;
import jakarta.ws.rs.core.Response;

public class LucilleResourceTest {

  private RunnerManager runnerManager;
  private LucilleResource lucilleResource;
  private Optional<PrincipalImpl> mockUser;

  @Before
  public void setUp() {
    runnerManager = RunnerManager.getInstance();
    // Use real AuthHandler, disable auth for most tests (can enable as needed)
    AuthHandler authHandler = new AuthHandler(false);
    lucilleResource = new LucilleResource(runnerManager, authHandler);
    mockUser = Optional.of(new PrincipalImpl("testUser"));
  }

  @Test
  public void testCreateRun_Success() {
    Map<String, Object> configBody = new HashMap<>();
    configBody.put("connectors", Collections.singletonList("dummyConnector"));
    Response response = lucilleResource.createConfig(mockUser, configBody);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Map<String, Object> responseBody = (Map<String, Object>) response.getEntity();
    assertNotNull(responseBody.get("configId"));
  }

  @Test
  public void testCreateRun_AuthFailure() {
    // Enable auth and use empty user
    AuthHandler authHandler = new AuthHandler(true);
    LucilleResource lucilleResourceWithAuth = new LucilleResource(runnerManager, authHandler);
    Map<String, Object> configBody = new HashMap<>();
    Optional<PrincipalImpl> noUser = Optional.empty();
    Response response = lucilleResourceWithAuth.createConfig(noUser, configBody);
    assertEquals(Response.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());
  }

  @Test
  public void testStartRun_Success() {
    // Create a config first
    Map<String, Object> configBody = new HashMap<>();
    configBody.put("pipeline", "testPipeline");
    Response configResponse = lucilleResource.createConfig(mockUser, configBody);
    String configId = (String) ((Map<?, ?>) configResponse.getEntity()).get("configId");
    // Start run
    Map<String, String> requestBody = new HashMap<>();
    requestBody.put("configId", configId);
    Response runResponse = lucilleResource.startRun(mockUser, requestBody);
    assertEquals(Response.Status.OK.getStatusCode(), runResponse.getStatus());
    RunDetails runDetails = (RunDetails) runResponse.getEntity();
    assertNotNull(runDetails);
    assertEquals(configId, runDetails.getConfigId());
  }

  @Test
  public void testStartRun_InvalidConfigId() {
    // Invalid configId
    Map<String, String> requestBody = new HashMap<>();
    requestBody.put("configId", "invalid-config-id");
    Response response = lucilleResource.startRun(mockUser, requestBody);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  public void testGetRunById_Success() {
    // Create and start a run
    Map<String, Object> configBody = new HashMap<>();
    configBody.put("pipeline", "testPipeline");
    Response configResponse = lucilleResource.createConfig(mockUser, configBody);
    String configId = (String) ((Map<?, ?>) configResponse.getEntity()).get("configId");
    Map<String, String> requestBody = new HashMap<>();
    requestBody.put("configId", configId);
    Response runResponse = lucilleResource.startRun(mockUser, requestBody);
    RunDetails runDetails = (RunDetails) runResponse.getEntity();
    String runId = runDetails.getRunId();
    // Fetch run details
    Response fetchedRunResponse = lucilleResource.getRunById(mockUser, runId);
    assertEquals(Response.Status.OK.getStatusCode(), fetchedRunResponse.getStatus());
    RunDetails fetchedRunDetails = (RunDetails) fetchedRunResponse.getEntity();
    assertEquals(runId, fetchedRunDetails.getRunId());
    assertEquals(configId, fetchedRunDetails.getConfigId());
  }

  @Test
  public void testGetRunById_NotFound() {
    Response response = lucilleResource.getRunById(mockUser, "non-existent-run-id");
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }
}
