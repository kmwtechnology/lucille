package com.kmwllc.lucille.endpoints;

import jakarta.ws.rs.Path;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lucille Liveness Health Check Endpoint:
 *
 * - endpoint: '/livez'
 *   - GET: lucille liveness status (always OK)
 */
@Path("/livez")
@Produces(MediaType.APPLICATION_JSON)
public class LucilleLivenessResource {

  private static final Logger log = LoggerFactory.getLogger(LucilleLivenessResource.class);


  public LucilleLivenessResource() {
    super();
  }


  @GET
  public Response isAlive() {
    return Response.ok().build();
  }
}
