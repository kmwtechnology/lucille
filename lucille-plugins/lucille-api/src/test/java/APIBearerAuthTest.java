import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import com.kmwllc.lucille.APIApplication;
import com.kmwllc.lucille.config.LucilleAPIConfiguration;
import com.kmwllc.lucille.objects.LoginRequest;
import com.kmwllc.lucille.objects.LoginResponse;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Base64;
import org.glassfish.jersey.client.ClientProperties;
import org.junit.ClassRule;
import org.junit.Test;

/** End-to-end auth test against a running app with auth enabled (test-conf-auth-coverage.yml, password
 "password"). Verifies the /v1/login -> bearer-token -> protected-endpoint flow, that Basic still works
 alongside Bearer, and that bad/absent tokens are rejected. 
*/
public class APIBearerAuthTest {

  @ClassRule
  public static final DropwizardAppRule<LucilleAPIConfiguration> RULE = new DropwizardAppRule<>(
      APIApplication.class, ResourceHelpers.resourceFilePath("test-conf-auth-coverage.yml"));

  private static final String BASIC =
      "Basic " + Base64.getEncoder().encodeToString("admin:password".getBytes());

  private final Client client = RULE.client().property(ClientProperties.READ_TIMEOUT, 30_000);
  private final String url = String.format("http://localhost:%d", RULE.getLocalPort());

  private String login(String password) {
    LoginRequest req = new LoginRequest();
    req.setPassword(password);
    Response response = client.target(url + "/v1/login").request()
        .post(Entity.entity(req, MediaType.APPLICATION_JSON));
    assertEquals(200, response.getStatus());
    return response.readEntity(LoginResponse.class).getToken();
  }

  @Test
  public void testLoginReturnsTokenThatAccessesProtectedEndpoint() {
    String token = login("password");
    assertNotNull(token);

    int status = client.target(url + "/v1/config").request()
        .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
        .get().getStatus();

    assertNotEquals("A valid bearer token should be accepted on /v1/config.", 401, status);
    assertEquals(200, status);
  }

  @Test
  public void testLoginWithWrongPasswordReturns401() {
    LoginRequest req = new LoginRequest();
    req.setPassword("wrong");
    Response response = client.target(url + "/v1/login").request()
        .post(Entity.entity(req, MediaType.APPLICATION_JSON));
    assertEquals(401, response.getStatus());
  }

  @Test
  public void testBasicAuthStillWorksAlongsideBearer() {
    int status = client.target(url + "/v1/config").request()
        .header(HttpHeaders.AUTHORIZATION, BASIC)
        .get().getStatus();
    assertEquals(200, status);
  }

  @Test
  public void testGarbageBearerTokenIsRejected() {
    int status = client.target(url + "/v1/config").request()
        .header(HttpHeaders.AUTHORIZATION, "Bearer not-a-real-token")
        .get().getStatus();
    assertEquals(401, status);
  }

  @Test
  public void testNoCredentialsIsRejected() {
    int status = client.target(url + "/v1/config").request().get().getStatus();
    assertEquals(401, status);
  }
}
