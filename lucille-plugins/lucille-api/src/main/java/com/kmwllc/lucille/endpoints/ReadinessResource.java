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
 * Lucille Readiness Health Check Endpoint:
 *
 * - endpoint: '/readyz'
 *   - GET: lucille readiness status (always OK)
 */
@Path("/v1/readyz")
@Tag(name = "Health", description = "Health info.")
@Produces(MediaType.APPLICATION_JSON)
@PermitAll
public class ReadinessResource {

  private static final Logger log = LoggerFactory.getLogger(ReadinessResource.class);

  public ReadinessResource() {
    super();
  }

  @GET
  @Operation(summary = "Readiness Check", description = "Returns 204 No Content to indicate the service is ready.")
  public Response isReady() {
    log.debug("Readiness check endpoint accessed.");
    // return Response.noContent().build();
    return Response.ok().build();
  }
}
