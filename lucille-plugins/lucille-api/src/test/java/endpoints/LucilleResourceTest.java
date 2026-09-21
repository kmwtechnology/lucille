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
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import com.kmwllc.lucille.core.RunDetails;
import com.kmwllc.lucille.core.RunnerManager;
import com.kmwllc.lucille.endpoints.LucilleResource;

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

  @Before
  public void setUp() {
    runnerManager = RunnerManager.getInstance();
    lucilleResource = new LucilleResource(runnerManager);
  }

  @Test
  public void testCreateRun_Success() {
    String configBody = "connectors = [\"dummyConnector\"]";
    Response response = lucilleResource.createConfig(configBody);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    CreateConfigResult result = (CreateConfigResult) response.getEntity();

    assertNotNull(result.getConfigId());
  }

  @Test
  public void testStartRun_Success() {
    // Create a config first
    String configBody = "pipeline = \"testPipeline\"";
    Response configResponse = lucilleResource.createConfig(configBody);
    String configId = ((CreateConfigResult) configResponse.getEntity()).getConfigId();
    // Start run
    RunRequest runRequest = new RunRequest();
    runRequest.setConfigId(configId);
    Response runResponse = lucilleResource.startRun(runRequest);
    assertEquals(Response.Status.OK.getStatusCode(), runResponse.getStatus());
    RunDetails runDetails = (RunDetails) runResponse.getEntity();
    assertNotNull(runDetails);
    assertEquals(configId, runDetails.getConfigId());
  }

  @Test
  public void testStartRunWithLockConfig() {
    LucilleResource preventConcurrentResource = new LucilleResource(runnerManager, true, Map.of());
    Response configResponse = preventConcurrentResource.createConfig(SLEEP_JSON);
    String configId = ((CreateConfigResult) configResponse.getEntity()).getConfigId();

    RunRequest runRequest = new RunRequest();
    runRequest.setConfigId(configId);

    // Similar to RunnerManager / API tests, there should be enough room
    // to prevent race conditions causing a failure here.
    Response runResponse1 = preventConcurrentResource.startRun(runRequest);
    assertEquals(Response.Status.OK.getStatusCode(), runResponse1.getStatus());

    Response runResponse2 = preventConcurrentResource.startRun(runRequest);
    assertEquals(Status.BAD_REQUEST.getStatusCode(), runResponse2.getStatus());

    String errorMessage = ((Map<String, String>) runResponse2.getEntity()).get("message");
    assertTrue(errorMessage.contains("is locked, in use by run"));
  }

  @Test
  public void testStartRun_InvalidConfigId() {
    // Invalid configId
    RunRequest runRequest = new RunRequest();
    runRequest.setConfigId("invalid-config-id");
    Response response = lucilleResource.startRun(runRequest);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  public void testGetRunById_Success() {
    // Create and start a run
    String configBody = "pipeline = \"testPipeline\"";
    Response configResponse = lucilleResource.createConfig(configBody);
    String configId = ((CreateConfigResult) configResponse.getEntity()).getConfigId();
    RunRequest runRequest = new RunRequest();
    runRequest.setConfigId(configId);
    Response runResponse = lucilleResource.startRun(runRequest);
    RunDetails runDetails = (RunDetails) runResponse.getEntity();
    String runId = runDetails.getRunId();
    // Fetch run details
    Response fetchedRunResponse = lucilleResource.getRunById(runId);
    assertEquals(Response.Status.OK.getStatusCode(), fetchedRunResponse.getStatus());
    RunDetails fetchedRunDetails = (RunDetails) fetchedRunResponse.getEntity();
    assertEquals(runId, fetchedRunDetails.getRunId());
    assertEquals(configId, fetchedRunDetails.getConfigId());
  }

  @Test
  public void testGetRunById_SuccessJsonConfig() {
    // Create and start a run
    String configBody = "{\"pipeline\": \"testPipeline\"}";
    Response configResponse = lucilleResource.createConfig(configBody);
    String configId = ((CreateConfigResult) configResponse.getEntity()).getConfigId();
    RunRequest runRequest = new RunRequest();
    runRequest.setConfigId(configId);
    Response runResponse = lucilleResource.startRun(runRequest);
    RunDetails runDetails = (RunDetails) runResponse.getEntity();
    String runId = runDetails.getRunId();
    // Fetch run details
    Response fetchedRunResponse = lucilleResource.getRunById(runId);
    assertEquals(Response.Status.OK.getStatusCode(), fetchedRunResponse.getStatus());
    RunDetails fetchedRunDetails = (RunDetails) fetchedRunResponse.getEntity();
    assertEquals(runId, fetchedRunDetails.getRunId());
    assertEquals(configId, fetchedRunDetails.getConfigId());
  }

  @Test
  public void testGetRunById_NotFound() {
    Response response = lucilleResource.getRunById("non-existent-run-id");
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

    LucilleResource presetResource = new LucilleResource(runnerManager, false, presetMap);

    Response resp2 = presetResource.getConfig("config2");
    Map<String, Object> respConf2 = (Map<String, Object>) resp2.getEntity();
    assertEquals(2, respConf2.get("id"));

    Response resp3 = presetResource.getConfig("config3");
    Map<String, Object> respConf3 = (Map<String, Object>) resp3.getEntity();
    assertEquals(3, respConf3.get("id"));
  }

  @Test
  public void testDeleteConfigs() {
    String configBody = "pipeline = \"testPipeline\"";
    Response configResponse = lucilleResource.createConfig(configBody);
    String configId = ((CreateConfigResult) configResponse.getEntity()).getConfigId();

    Response deleteResponse = lucilleResource.deleteConfig(configId);
    assertEquals(Response.Status.OK.getStatusCode(), deleteResponse.getStatus());

    Response badDeleteResponse = lucilleResource.getConfig(UUID.randomUUID().toString());
    assertEquals(Status.NOT_FOUND.getStatusCode(), badDeleteResponse.getStatus());
  }
}
