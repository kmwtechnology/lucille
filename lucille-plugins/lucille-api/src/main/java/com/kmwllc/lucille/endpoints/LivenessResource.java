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
 * Lucille Liveness Health Check Endpoint.
 * <p>
 * Provides a simple endpoint to check if the Lucille API service is running.
 * <ul>
 *   <li>endpoint: '/livez'</li>
 *   <li>GET: returns 204 No Content if live</li>
 * </ul>
 */
@Path("/v1/livez")
@Tag(name = "Health", description = "Health info.")
@Produces(MediaType.APPLICATION_JSON)
@PermitAll
public class LivenessResource {

  /**
   * Logger for the LivenessResource.
   */
  private static final Logger log = LoggerFactory.getLogger(LivenessResource.class);

  /**
   * Constructs a new LivenessResource.
   */
  public LivenessResource() {
    super();
  }

  /**
   * Liveness check endpoint.
   * @return HTTP 204 No Content if the service is live
   */
  @GET
  @Operation(summary = "Liveness Check", description = "Returns 204 No Content to indicate the service is live.")
  public Response isAlive() {
    log.debug("Liveness check endpoint accessed.");
    return Response.ok().build();
  }
}
