package com.kmwllc.lucille.endpoints;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

/**
 * Lucille Liveness Health Check Endpoint:
 *
 * - endpoint: '/livez'
 *   - GET: lucille liveness status (always OK)
 */
@Path("/v1/livez")
@Tag(name = "Health", description = "Health info.")
@Produces(MediaType.APPLICATION_JSON)
@PermitAll
public class LivenessResource {

  private static final Logger log = LoggerFactory.getLogger(LivenessResource.class);

  public LivenessResource() {
    super();
  }

  @GET
  @Operation(summary = "Liveness Check", description = "Returns 204 No Content to indicate the service is live.")
  public Response isAlive() {
    log.debug("Liveness check endpoint accessed.");
    // return Response.noContent().build();
    return Response.ok().build();
  }
}
