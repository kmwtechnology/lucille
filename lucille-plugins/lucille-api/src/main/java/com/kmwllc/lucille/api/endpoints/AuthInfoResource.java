package com.kmwllc.lucille.api.endpoints;

import com.kmwllc.lucille.api.auth.AuthNotRequired;
import com.kmwllc.lucille.api.objects.AuthInfoResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

/**
 * Reports whether the API requires authentication. Reachable without a token so the Admin UI can
 * decide, before login, whether to show a login screen at all.
 */
@Path("/v1/auth-info")
@AuthNotRequired
@Produces(MediaType.APPLICATION_JSON)
public class AuthInfoResource {

  private final boolean authEnabled;

  /**
   * Constructs an AuthInfoResource.
   * @param authEnabled whether the API requires authentication
   */
  public AuthInfoResource(boolean authEnabled) {
    this.authEnabled = authEnabled;
  }

  /**
   * Returns the API's authentication state.
   * @return HTTP 200 with an {@link AuthInfoResponse}
   */
  @GET
  @Tag(name = "Auth")
  @Operation(summary = "Report whether authentication is required",
      description = "Returns whether the API requires authentication, so clients know to prompt for login.")
  public Response authInfo() {
    return Response.ok(new AuthInfoResponse(authEnabled)).build();
  }
}
