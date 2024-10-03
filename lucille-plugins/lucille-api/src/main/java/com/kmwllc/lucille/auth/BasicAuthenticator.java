package com.kmwllc.lucille.auth;

import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.PrincipalImpl;
import io.dropwizard.auth.basic.BasicCredentials;
import java.util.Optional;

/**
 * Authenticator for Basic Auth, only checks that the password is correct and the creates a new user with the given name
 */
public class BasicAuthenticator implements Authenticator<BasicCredentials, PrincipalImpl> {

  private final String password;

  public BasicAuthenticator(String password) {
    super();
    this.password = password;
  }

  @Override
  public Optional<PrincipalImpl> authenticate(BasicCredentials credentials) throws AuthenticationException {
    if (password != null && password.equals(credentials.getPassword())) {
      return Optional.of(new PrincipalImpl(credentials.getUsername()));
    } else {
      return Optional.empty();
    }
  }
}
