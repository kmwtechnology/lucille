package endpoints;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.objects.RunRequest;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.kmwllc.lucille.core.CreateConfigResult;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import com.kmwllc.lucille.AuthHandler;
import com.kmwllc.lucille.core.RunDetails;
import com.kmwllc.lucille.core.RunnerManager;
import com.kmwllc.lucille.endpoints.LucilleResource;
import io.dropwizard.auth.PrincipalImpl;

public class LucilleResourceTest {

  private static final String SLEEP_JSON = """
  {
    "connectors": [
      {
        "class": "com.kmwllc.lucille.connector.SleepConnector",
        "name": "sleep-connector",
        "pipeline": "pipeline1",
        "duration": 100
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
      "class": "com.kmwllc.lucille.indexer.NopIndexer",
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
    CreateConfigResult result = (CreateConfigResult) response.getEntity();

    assertNotNull(result.getConfigId());
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
    String configId = ((CreateConfigResult) configResponse.getEntity()).getConfigId();
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
    LucilleResource preventConcurrentResource = new LucilleResource(runnerManager, new AuthHandler(false), true, Map.of());
    Response configResponse = preventConcurrentResource.createConfig(mockUser, SLEEP_JSON);
    String configId = ((CreateConfigResult) configResponse.getEntity()).getConfigId();

    RunRequest runRequest = new RunRequest();
    runRequest.setConfigId(configId);

    // Similar to RunnerManager / API tests, there should be enough room
    // to prevent race conditions causing a failure here.
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
    String configId = ((CreateConfigResult) configResponse.getEntity()).getConfigId();
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
    String configId = ((CreateConfigResult) configResponse.getEntity()).getConfigId();
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

  @Test
  public void testPresetConfigs() {
    Config config2 = ConfigFactory.load("test_presets/config2.conf");
    Config config3 = ConfigFactory.load("test_presets/config3.json");

    Map<String, Config> presetMap = Map.of(
        "config2", config2,
        "config3", config3
    );

    AuthHandler authHandler = new AuthHandler(false);
    LucilleResource presetResource = new LucilleResource(runnerManager, authHandler, false, presetMap);

    Response resp2 = presetResource.getConfig(mockUser, "config2");
    Map<String, Object> respConf2 = (Map<String, Object>) resp2.getEntity();
    assertEquals(2, respConf2.get("id"));

    Response resp3 = presetResource.getConfig(mockUser, "config3");
    Map<String, Object> respConf3 = (Map<String, Object>) resp3.getEntity();
    assertEquals(3, respConf3.get("id"));
  }

  @Test
  public void testDeleteConfigs() {
    String configBody = "pipeline = \"testPipeline\"";
    Response configResponse = lucilleResource.createConfig(mockUser, configBody);
    String configId = ((CreateConfigResult) configResponse.getEntity()).getConfigId();

    Response deleteResponse = lucilleResource.deleteConfig(mockUser, configId);
    assertEquals(Response.Status.OK.getStatusCode(), deleteResponse.getStatus());

    Response badDeleteResponse = lucilleResource.getConfig(mockUser, UUID.randomUUID().toString());
    assertEquals(Status.NOT_FOUND.getStatusCode(), badDeleteResponse.getStatus());
  }
}
