package com.kmwllc.lucille.auth;


import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.PrincipalImpl;
import io.dropwizard.auth.basic.BasicCredentials;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicAuthenticator implements Authenticator<BasicCredentials, PrincipalImpl> {

  private static final Logger log = LoggerFactory.getLogger(BasicAuthenticator.class);
  private final String password;

  public BasicAuthenticator(String password) {
    super();
    this.password = password;
  }

  @Override
  public Optional<PrincipalImpl> authenticate(BasicCredentials credentials) throws AuthenticationException {
    if (password.equals(credentials.getPassword())) {
      return Optional.of(new PrincipalImpl(credentials.getUsername()));
    } else {
      return Optional.empty();
    }
  }
}
