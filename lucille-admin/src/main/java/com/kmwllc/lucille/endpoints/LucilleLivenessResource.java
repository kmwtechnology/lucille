package com.kmwllc.lucille.endpoints;

import com.codahale.metrics.health.HealthCheck;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/livez")
public class LucilleLivenessResource extends HealthCheck {

  private static final Logger log = LoggerFactory.getLogger(LucilleLivenessResource.class);


  public LucilleLivenessResource() {
    super();
  }


  // The Liveness endpoint can serve as both the 'livez' endpoint for Kubernetes and the
  // HealthCheck endpoint for Dropwizard
  @Override
  public Result check() throws Exception {
    return Result.healthy();
  }

  @GET
  public Response isAlive() {
    return Response.ok().build();
  }
}
