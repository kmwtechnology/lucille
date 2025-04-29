package com.kmwllc.lucille;

import java.util.Optional;
import io.dropwizard.auth.PrincipalImpl;
import jakarta.ws.rs.core.Response;

/**
 * Handles authentication logic for Lucille API requests.
 * <p>
 * If authentication is enabled, this class checks for a valid user principal on each request.
 */
public class AuthHandler {

  /**
   * Whether authentication is enabled for the API.
   */
  private final boolean authEnabled;

  /**
   * Constructs an AuthHandler.
   * @param authEnabled true to enable authentication, false to disable
   */
  public AuthHandler(boolean authEnabled) {
    this.authEnabled = authEnabled;
  }

  /**
   * Handles authentication for a request.
   * 
   * @param user Optional user principal
   * @return null if authentication passes; a Response if authentication fails
   */
  public Response authenticate(Optional<PrincipalImpl> user) {
    if (authEnabled && user.isEmpty()) {
      return Response.status(Response.Status.UNAUTHORIZED)
          .entity("User authentication is required.").build();
    }
    return null; // Authentication passed
  }
}
