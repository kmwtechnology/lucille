package endpoints;

import static org.junit.Assert.assertEquals;

import com.kmwllc.lucille.auth.JwtService;
import com.kmwllc.lucille.endpoints.LoginResource;
import com.kmwllc.lucille.objects.LoginRequest;
import com.kmwllc.lucille.objects.LoginResponse;
import jakarta.ws.rs.core.Response;
import org.junit.Test;

public class LoginResourceTest {

  private final JwtService jwtService = new JwtService();
  private final LoginResource resource = new LoginResource(jwtService, "password");

  private static LoginRequest request(String password) {
    LoginRequest req = new LoginRequest();
    req.setPassword(password);
    return req;
  }

  @Test
  public void testValidPasswordReturnsVerifiableToken() {
    Response response = resource.login(request("password"));

    assertEquals(200, response.getStatus());
    LoginResponse body = (LoginResponse) response.getEntity();
    assertEquals("admin", jwtService.verifyToken(body.getToken()).orElse(null));
  }

  @Test
  public void testWrongPasswordReturns401() {
    assertEquals(401, resource.login(request("wrong")).getStatus());
  }

  @Test
  public void testNullRequestReturns401() {
    assertEquals(401, resource.login(null).getStatus());
  }

  @Test
  public void testMissingPasswordReturns401() {
    assertEquals(401, resource.login(request(null)).getStatus());
  }

  @Test
  public void testRejectsAllLoginsWhenNoPasswordConfigured() {
    LoginResource noPassword = new LoginResource(jwtService, null);
    assertEquals(401, noPassword.login(request("password")).getStatus());
  }
}
