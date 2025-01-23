import static org.junit.Assert.assertEquals;

import com.kmwllc.lucille.APIApplication;
import com.kmwllc.lucille.config.LucilleAPIConfiguration;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.jupiter.api.BeforeAll;
import org.apache.logging.log4j.core.config.Configurator;



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


}
