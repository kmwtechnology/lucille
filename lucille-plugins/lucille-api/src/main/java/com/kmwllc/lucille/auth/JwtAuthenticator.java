package com.kmwllc.lucille.auth;

import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.PrincipalImpl;
import java.util.Objects;
import java.util.Optional;

/**
 * Authenticates a bearer token by verifying it with {@link JwtService}. The credential supplied by
 * Dropwizard's OAuth filter is the raw token string; a valid token yields a principal named after
 * the token subject, and an invalid or expired token yields an empty result (a 401).
 */
public class JwtAuthenticator implements Authenticator<String, PrincipalImpl> {

  private final JwtService jwtService;

  /**
   * Constructs a JwtAuthenticator backed by the given JwtService.
   * @param jwtService the service used to verify tokens
   */
  public JwtAuthenticator(JwtService jwtService) {
    this.jwtService = Objects.requireNonNull(jwtService, "A JwtService is required.");
  }

  /**
   * Authenticates the given bearer token.
   * @param token the raw bearer token
   * @return a principal when the token is valid; otherwise empty
   * @throws AuthenticationException never; invalid tokens return empty rather than throwing
   */
  @Override
  public Optional<PrincipalImpl> authenticate(String token) throws AuthenticationException {
    return jwtService.verifyToken(token).map(PrincipalImpl::new);
  }
}
