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
 * Lucille Readiness Health Check Endpoint.
 * <p>
 * Provides a simple endpoint to check if the Lucille API service is ready to receive traffic.
 * <ul>
 *   <li>endpoint: '/readyz'</li>
 *   <li>GET: returns 204 No Content if ready</li>
 * </ul>
 */
@Path("/v1/readyz")
@Tag(name = "Health", description = "Health info.")
@Produces(MediaType.APPLICATION_JSON)
@PermitAll
public class ReadinessResource {

  /**
   * Logger for the ReadinessResource.
   */
  private static final Logger log = LoggerFactory.getLogger(ReadinessResource.class);

  /**
   * Constructs a new ReadinessResource.
   */
  public ReadinessResource() {
    super();
  }

  /**
   * Readiness check endpoint.
   * @return HTTP 204 No Content if the service is ready
   */
  @GET
  @Operation(summary = "Readiness Check", description = "Returns 204 No Content to indicate the service is ready.")
  public Response isReady() {
    log.debug("Readiness check endpoint accessed.");
    return Response.ok().build();
  }
}
