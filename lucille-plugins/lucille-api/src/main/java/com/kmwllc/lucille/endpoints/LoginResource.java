package com.kmwllc.lucille.endpoints;

import com.kmwllc.lucille.auth.AuthNotRequired;
import com.kmwllc.lucille.auth.JwtService;
import com.kmwllc.lucille.objects.LoginRequest;
import com.kmwllc.lucille.objects.LoginResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Login endpoint for the Admin UI. Exchanges the shared admin password for a short-lived bearer
 * token, which clients then send as {@code Authorization: Bearer <token>}. Reachable without a
 * token, since there would otherwise be no way to obtain one.
 */
@Path("/v1/login")
@AuthNotRequired
@Produces(MediaType.APPLICATION_JSON)
public class LoginResource {

  private static final Logger log = LoggerFactory.getLogger(LoginResource.class);

  /** Subject assigned to issued tokens; there is a single shared account. */
  private static final String SUBJECT = "admin";

  private final JwtService jwtService;
  private final String expectedPassword;

  /**
   * Constructs a LoginResource.
   * @param jwtService the service used to issue tokens
   * @param expectedPassword the configured shared password; when null, all logins are rejected
   */
  public LoginResource(JwtService jwtService, String expectedPassword) {
    this.jwtService = Objects.requireNonNull(jwtService, "A JwtService is required.");
    this.expectedPassword = expectedPassword;
  }

  /**
   * Authenticates a password and returns a bearer token on success.
   * @param request the login request containing the password
   * @return HTTP 200 with a {@link LoginResponse} token, or 401 when the password is invalid
   */
  @POST
  @Tag(name = "Auth")
  @Consumes(MediaType.APPLICATION_JSON)
  @Operation(summary = "Log in and obtain a bearer token",
      description = "Validates the supplied password and returns a short-lived JWT bearer token.")
  public Response login(LoginRequest request) {
    if (request == null || request.getPassword() == null
        || expectedPassword == null || !expectedPassword.equals(request.getPassword())) {
      return ResponseUtils.buildErrorResponse(Response.Status.UNAUTHORIZED, "Invalid credentials.");
    }

    String token = jwtService.issueToken(SUBJECT);
    log.info("Issued an admin token.");
    return Response.ok(new LoginResponse(token)).build();
  }
}
