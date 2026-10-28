package auth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.auth.JwtAuthenticator;
import com.kmwllc.lucille.auth.JwtService;
import io.dropwizard.auth.PrincipalImpl;
import java.util.Optional;
import org.junit.Test;

public class JwtAuthenticatorTest {

  private final JwtService jwtService = new JwtService();
  private final JwtAuthenticator authenticator = new JwtAuthenticator(jwtService);

  @Test
  public void testValidTokenYieldsPrincipalNamedAfterSubject() throws Exception {
    String token = jwtService.issueToken("alice");

    Optional<PrincipalImpl> principal = authenticator.authenticate(token);

    assertTrue(principal.isPresent());
    assertEquals("alice", principal.get().getName());
  }

  @Test
  public void testInvalidTokenYieldsEmpty() throws Exception {
    assertTrue(authenticator.authenticate("bogus").isEmpty());
  }

  @Test
  public void testTokenFromAnotherServiceYieldsEmpty() throws Exception {
    String foreignToken = new JwtService().issueToken("alice");
    assertTrue(authenticator.authenticate(foreignToken).isEmpty());
  }
}
