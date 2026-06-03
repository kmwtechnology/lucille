import static org.junit.Assert.assertEquals;

import com.kmwllc.lucille.APIApplication;
import com.kmwllc.lucille.config.LucilleAPIConfiguration;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Base64;
import org.junit.ClassRule;
import org.junit.Test;

public class APIPreventConcurrentIntegrationTest {

  private static final String SLEEP_JSON = """
  {
    "connectors": [
      {
        "class": "connector.SleepConnector",
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

  @ClassRule
  public static final DropwizardAppRule<LucilleAPIConfiguration> RULE = new DropwizardAppRule<>(
      APIApplication.class, ResourceHelpers.resourceFilePath("test-conf-prevent-concurrent.yml"));

  private final Client client = RULE.client();
  private final String url = String.format("http://localhost:%d/", RULE.getLocalPort());
  private final String authHeader =
      "Basic " + Base64.getEncoder().encodeToString("admin:password".getBytes());

  @Test
  public void testPreventConcurrentRuns() throws Exception {
    Response configStatus1 = client.target(url + "v1/config").request()
        .header(HttpHeaders.AUTHORIZATION, authHeader).post(Entity.entity(SLEEP_JSON, MediaType.APPLICATION_JSON));
    String configResponse1 = configStatus1.readEntity(String.class);
    // same retrieval as in APIIntegrationTest
    String configId1 = configResponse1.substring(13, configResponse1.length() - 2);

    Response configStatus2 = client.target(url + "v1/config").request()
        .header(HttpHeaders.AUTHORIZATION, authHeader).post(Entity.entity(SLEEP_JSON, MediaType.APPLICATION_JSON));
    String configResponse2 = configStatus2.readEntity(String.class);
    // same retrieval as in APIIntegrationTest
    String configId2 = configResponse2.substring(13, configResponse2.length() - 2);

    Response runPostStatus = client.target(url + "v1/run").request()
        .header(HttpHeaders.AUTHORIZATION, authHeader).post(Entity.entity("{\"configId\": \"" + configId1 + "\"}", MediaType.APPLICATION_JSON));
    assertEquals(200, runPostStatus.getStatus());
    System.out.println("ENTITY 1: " + runPostStatus.readEntity(String.class));

    // identical config, but referenced by a different ID, so it can run.
    Response runPostStatus2 = client.target(url + "v1/run").request()
        .header(HttpHeaders.AUTHORIZATION, authHeader).post(Entity.entity("{\"configId\": \"" + configId2 + "\"}", MediaType.APPLICATION_JSON));
    assertEquals(200, runPostStatus2.getStatus());

    Response rejectedStatus = client.target(url + "v1/run").request()
        .header(HttpHeaders.AUTHORIZATION, authHeader).post(Entity.entity("{\"configId\": \"" + configId1 + "\"}", MediaType.APPLICATION_JSON));
    System.out.println("ENTITY 2: " + rejectedStatus.readEntity(String.class));
    assertEquals(400, rejectedStatus.getStatus());
  }
}
