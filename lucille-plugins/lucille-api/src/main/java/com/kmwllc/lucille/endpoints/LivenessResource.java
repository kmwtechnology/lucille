package com.kmwllc.lucille.endpoints;

import io.dropwizard.auth.Auth;
import io.dropwizard.auth.PrincipalImpl;
import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Optional;
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
@PermitAll
public class LivenessResource {

  private static final Logger log = LoggerFactory.getLogger(LivenessResource.class);


  public LivenessResource() {
    super();
  }

  @GET
  public Response isAlive(@Auth Optional<PrincipalImpl> user) {
    if (user.isPresent()) {
      return Response.ok().build();
    } else {
      return Response.status(401).build();
    }
  }
}
