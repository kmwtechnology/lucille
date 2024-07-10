package com.kmwllc.lucille.resources;

import com.codahale.metrics.health.HealthCheck;
import com.kmwllc.lucille.core.RunnerManager;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LucilleReadinessResource {
  private static final Logger log = LoggerFactory.getLogger(LucilleReadinessResource.class);
  private final RunnerManager rm;


  public LucilleReadinessResource() {
    super();
    rm = RunnerManager.getInstance();
  }

  @GET
  public Response isReady() {
    boolean isRunning = rm.isRunning();

    if (isRunning) {
      return Response.ok().build();
    } else {
      // TODO : What response do we want to return when we are not ready?
      return Response.status(425).build();
    }
  }
}