import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.APIApplication;
import com.kmwllc.lucille.config.LucilleAPIConfiguration;
import com.kmwllc.lucille.core.CreateConfigResult;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Base64;
import java.util.Map;
import org.junit.ClassRule;
import org.junit.Test;

public class APIIntegrationTest {

  @ClassRule
  public static final DropwizardAppRule<LucilleAPIConfiguration> RULE = new DropwizardAppRule<>(
      APIApplication.class, ResourceHelpers.resourceFilePath("test-conf.yml"));

  private final Client client = RULE.client();
  private final String url = String.format("http://localhost:%d/", RULE.getLocalPort());
  private final String authHeader =
      "Basic " + Base64.getEncoder().encodeToString("admin:password".getBytes());

  @Test
  public void testAuthSuccessful() {
    Response status = client.target(url + "v1/config").request()
        .header(HttpHeaders.AUTHORIZATION, authHeader).get();
    assertEquals(200, status.getStatus());
  }

  @Test
  public void testAuthWrongPassword() {
    String badAuth = "Basic " + Base64.getEncoder().encodeToString("wrong:wrong".getBytes());
    Response status =
        client.target(url + "v1/config").request().header(HttpHeaders.AUTHORIZATION, badAuth).get();

    assertEquals(401, status.getStatus());
  }

  @Test
  public void testAuthNoPassword() {
    Response status = client.target(url + "v1/config").request().get();

    assertEquals(401, status.getStatus());
  }

  @Test
  public void testReadiness() {
    Response status = client.target(url + "v1/readyz").request()
        .header(HttpHeaders.AUTHORIZATION, authHeader).get();

    assertEquals(200, status.getStatus());;
  }

  @Test
  public void testLiveness() {
    Response status = client.target(url + "v1/livez").request()
        .header(HttpHeaders.AUTHORIZATION, authHeader).get();

    assertEquals(200, status.getStatus());
  }

  @Test
  public void testSystemStats() {
    Response status = client.target(url + "v1/systemstats").request()
        .header(HttpHeaders.AUTHORIZATION, authHeader).get();
    assertEquals(200, status.getStatus());
    // Optionally, further assertions can be added here to check the response body/fields
  }

  @Test
  public void testDropwizardMetrics() {
    Response status = client.target(url + "v1/systemstats/metrics").request()
        .header(HttpHeaders.AUTHORIZATION, authHeader).get();
    assertEquals(200, status.getStatus());
  }

  @Test
  public void testConfigInfoConnectorList() {
    Response status = client.target(url + "v1/config-info/connector-list").request()
        .header(HttpHeaders.AUTHORIZATION, authHeader).get();
    assertEquals(200, status.getStatus());
  }

  @Test
  public void testConfigInfoStageList() {
    Response status = client.target(url + "v1/config-info/stage-list").request()
        .header(HttpHeaders.AUTHORIZATION, authHeader).get();
    assertEquals(200, status.getStatus());
  }

  @Test
  public void testConfigInfoIndexerList() {
    Response status = client.target(url + "v1/config-info/indexer-list").request()
        .header(HttpHeaders.AUTHORIZATION, authHeader).get();
    assertEquals(200, status.getStatus());
  }

  // Creates config and uses it to hit run endpoint, allowing us to test POST requests with specific IDs
  @Test
  public void testConfigGenerationAndRunId() {
    Response configStatus = client.target(url + "v1/config").request()
        .header(HttpHeaders.AUTHORIZATION, authHeader).post(Entity.entity("{}", MediaType.APPLICATION_JSON));
    CreateConfigResult configResponse = configStatus.readEntity(CreateConfigResult.class);
    String configId = configResponse.getConfigId();

    assertNull(configResponse.getRemovedConfigId());

    Response configIdStatus = client.target(url + "v1/config/" + configId).request()
        .header(HttpHeaders.AUTHORIZATION, authHeader).get();
    assertEquals(200, configIdStatus.getStatus());

    Response runPostStatus = client.target(url + "v1/run").request()
        .header(HttpHeaders.AUTHORIZATION, authHeader).post(Entity.entity(configResponse, MediaType.APPLICATION_JSON));
    assertEquals(200, runPostStatus.getStatus());
    String runResponse = runPostStatus.readEntity(String.class);

    // This is the runResponse structure, and we just need the id part:
    // {"runId":"3944a016-9d95-487f-b47e-0030442a0251","configId":"340a7c70-c3e0-4c0c-af96-1414caf3623f","startTime":1779139586.541141000,
    // "endTime":null,"runResult":null,"runType":"LOCAL","done":false,"future":{"completedExceptionally":false,
    // "numberOfDependents":0,"done":false,"cancelled":false}}
    String runId = runResponse.substring(10, 46);

    Response runStatus = client.target(url + "v1/run").request()
        .header(HttpHeaders.AUTHORIZATION, authHeader).get();
    assertEquals(200, runStatus.getStatus());

    Response runIdStatus = client.target(url + "v1/run/" + runId).request()
        .header(HttpHeaders.AUTHORIZATION, authHeader).get();
    assertEquals(200, runIdStatus.getStatus());
  }

  @Test
  public void testPresetConfigPresence() {
    Response configGetStatus = client.target(url + "v1/config").request()
        .header(HttpHeaders.AUTHORIZATION, authHeader).get();

    Map<String, Object> configGetMap = configGetStatus.readEntity(Map.class);
    // presets folder has three configs: incremental(.conf), simple-csv-solr-example(.hocon), and simple-config(.json)
    // Similar to PresetConfigHandlerTest - bad.conf is not included and does not cause
    // issues initializing the API.
    assertTrue(configGetMap.containsKey("incremental.conf"));
    assertTrue(configGetMap.containsKey("simple-csv-solr-example.hocon"));
    assertTrue(configGetMap.containsKey("simple-config.json"));
  }
}
