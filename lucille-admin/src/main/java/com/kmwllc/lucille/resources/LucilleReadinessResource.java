package com.kmwllc.lucille.resources;

import com.codahale.metrics.health.HealthCheck;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LucilleReadinessResource {
  private static final Logger log = LoggerFactory.getLogger(LucilleReadinessResource.class);


  public LucilleReadinessResource() {
    super();
  }

  @GET
  public Response isReady() {
    return Response.status(200).build();
  }
}