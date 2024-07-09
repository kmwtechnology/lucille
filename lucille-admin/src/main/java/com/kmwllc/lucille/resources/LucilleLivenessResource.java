package com.kmwllc.lucille.resources;

import com.codahale.metrics.health.HealthCheck;
import jakarta.ws.rs.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LucilleLivenessResource extends HealthCheck {

  private static final Logger log = LoggerFactory.getLogger(LucilleLivenessResource.class);


  public LucilleLivenessResource() {
    super();
  }

  @Override
  public Result check() throws Exception {
    return Result.healthy();
  }
}
