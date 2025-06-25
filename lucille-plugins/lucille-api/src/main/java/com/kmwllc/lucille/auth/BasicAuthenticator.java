package com.kmwllc.lucille.auth;

import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.PrincipalImpl;
import io.dropwizard.auth.basic.BasicCredentials;
import java.util.Optional;

/**
 * Authenticator for Basic Auth. Checks that the provided password matches the expected password and creates a user principal if valid.
 */
public class BasicAuthenticator implements Authenticator<BasicCredentials, PrincipalImpl> {

  /**
   * The expected password for authentication.
   */
  private final String password;

  /**
   * Constructs a BasicAuthenticator with the provided password.
   * @param password the password to check against
   */
  public BasicAuthenticator(String password) {
    super();
    this.password = password;
  }

  /**
   * Authenticates the given credentials.
   * @param credentials the Basic Auth credentials (username and password)
   * @return an Optional containing a PrincipalImpl if authentication succeeds; otherwise, Optional.empty()
   * @throws AuthenticationException if an authentication error occurs
   */
  @Override
  public Optional<PrincipalImpl> authenticate(BasicCredentials credentials) throws AuthenticationException {
    if (password != null && password.equals(credentials.getPassword())) {
      return Optional.of(new PrincipalImpl(credentials.getUsername()));
    } else {
      return Optional.empty();
    }
  }
}
