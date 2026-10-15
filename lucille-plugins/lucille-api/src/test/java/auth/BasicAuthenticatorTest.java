package auth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.Optional;

import org.junit.Test;

import com.kmwllc.lucille.auth.BasicAuthenticator;

import io.dropwizard.auth.PrincipalImpl;
import io.dropwizard.auth.basic.BasicCredentials;

public class BasicAuthenticatorTest {

  private final BasicAuthenticator authenticator = new BasicAuthenticator("password");

  @Test
  public void testAuthSuccess() throws Exception {
    /**
     * The Authentication should pass when the configured password is given
     */
    Optional<PrincipalImpl> user = authenticator.authenticate(new BasicCredentials("test", "password"));
    assertTrue(user.isPresent());
    assertEquals("test", user.get().getName());
  }

  @Test
  public void testAuthFailure() throws Exception {
    /**
     * If the wrong password is given the Authentication should fail
     */
    Optional<PrincipalImpl> user = authenticator.authenticate(new BasicCredentials("test", "wrong"));
    assertTrue(user.isEmpty());
  }
}
