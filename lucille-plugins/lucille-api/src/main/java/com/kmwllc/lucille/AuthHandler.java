package com.kmwllc.lucille;

import java.util.Optional;
import io.dropwizard.auth.PrincipalImpl;
import jakarta.ws.rs.core.Response;

public class AuthHandler {

  private final boolean authEnabled;

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
