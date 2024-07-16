package com.kmwllc.lucille.endpoints;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
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
public class LucilleReadinessResource {
  private static final Logger log = LoggerFactory.getLogger(LucilleReadinessResource.class);

  public LucilleReadinessResource() {
    super();
  }

  @GET
  public Response isReady() {
    return Response.ok().build();
  }
}