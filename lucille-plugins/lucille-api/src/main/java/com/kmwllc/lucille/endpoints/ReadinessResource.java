package com.kmwllc.lucille.endpoints;

import io.dropwizard.auth.Auth;
import io.dropwizard.auth.PrincipalImpl;
import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lucille Readiness Health Check Endpoint:
 *
 * - endpoint: '/readyz'
 *   - GET: lucille readiness status (always OK)
 */
@Path("/readyz")
@Produces(MediaType.APPLICATION_JSON)
@PermitAll
public class ReadinessResource {
  private static final Logger log = LoggerFactory.getLogger(ReadinessResource.class);

  public ReadinessResource() {
    super();
  }

  @GET
  public Response isReady(@Auth Optional<PrincipalImpl> user) {
    return Response.ok().build();
  }
}