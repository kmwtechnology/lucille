package endpoints;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.objects.RunRequest;
import jakarta.ws.rs.core.Response.Status;
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

  private static final String SLEEP_JSON = """
  {
    "connectors": [
      {
        "class": "com.kmwllc.lucille.connector.SleepConnector",
        "name": "connector1",
        "pipeline": "pipeline1",
        "duration": 1000
      }
    ],
    "pipelines": [
      {
        "name": "pipeline1",
        "stages": []
      }
    ],
    "indexer": {
      "type": "NoOpIndexer",
      "class": "com.kmwllc.lucille.indexer.NopIndexer"
    }
  }
  """;

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
    String configBody = "connectors = [\"dummyConnector\"]";
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
    Optional<PrincipalImpl> noUser = Optional.empty();
    Response response = lucilleResourceWithAuth.createConfig(noUser, "");
    assertEquals(Response.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());
  }

  @Test
  public void testStartRun_Success() {
    // Create a config first
    String configBody = "pipeline = \"testPipeline\"";
    Response configResponse = lucilleResource.createConfig(mockUser, configBody);
    String configId = (String) ((Map<?, ?>) configResponse.getEntity()).get("configId");
    // Start run
    RunRequest runRequest = new RunRequest();
    runRequest.setConfigId(configId);
    Response runResponse = lucilleResource.startRun(mockUser, runRequest);
    assertEquals(Response.Status.OK.getStatusCode(), runResponse.getStatus());
    RunDetails runDetails = (RunDetails) runResponse.getEntity();
    assertNotNull(runDetails);
    assertEquals(configId, runDetails.getConfigId());
  }

  @Test
  public void testStartRunWithLockConfig() {
    LucilleResource preventConcurrentResource = new LucilleResource(runnerManager, new AuthHandler(false), true);
    Response configResponse = preventConcurrentResource.createConfig(mockUser, SLEEP_JSON);
    String configId = (String) ((Map<?, ?>) configResponse.getEntity()).get("configId");

    RunRequest runRequest = new RunRequest();
    runRequest.setConfigId(configId);
    Response runResponse1 = preventConcurrentResource.startRun(mockUser, runRequest);
    assertEquals(Response.Status.OK.getStatusCode(), runResponse1.getStatus());

    Response runResponse2 = preventConcurrentResource.startRun(mockUser, runRequest);
    assertEquals(Status.BAD_REQUEST.getStatusCode(), runResponse2.getStatus());

    String errorMessage = ((Map<String, String>) runResponse2.getEntity()).get("message");
    assertTrue(errorMessage.contains("is locked, in use by run"));
  }

  @Test
  public void testStartRun_InvalidConfigId() {
    // Invalid configId
    RunRequest runRequest = new RunRequest();
    runRequest.setConfigId("invalid-config-id");
    Response response = lucilleResource.startRun(mockUser, runRequest);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  public void testGetRunById_Success() {
    // Create and start a run
    String configBody = "pipeline = \"testPipeline\"";
    Response configResponse = lucilleResource.createConfig(mockUser, configBody);
    String configId = (String) ((Map<?, ?>) configResponse.getEntity()).get("configId");
    RunRequest runRequest = new RunRequest();
    runRequest.setConfigId(configId);
    Response runResponse = lucilleResource.startRun(mockUser, runRequest);
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
  public void testGetRunById_SuccessJsonConfig() {
    // Create and start a run
    String configBody = "{\"pipeline\": \"testPipeline\"}";
    Response configResponse = lucilleResource.createConfig(mockUser, configBody);
    String configId = (String) ((Map<?, ?>) configResponse.getEntity()).get("configId");
    RunRequest runRequest = new RunRequest();
    runRequest.setConfigId(configId);
    Response runResponse = lucilleResource.startRun(mockUser, runRequest);
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
