package com.kmwllc.lucille.resources;

import com.kmwllc.lucille.core.RunnerManager;
import com.kmwllc.lucille.objects.RunStatus;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;

import jakarta.ws.rs.core.Response;

public class LucilleAdminResource {

  private final RunnerManager rm;

  public LucilleAdminResource() {
    rm = RunnerManager.getInstance();
  }

  @GET
  public Response getRunStatus() {
    return Response.ok().build();
  }

  @POST
  public Response startRun() {
    return Response.ok("Lucille has been started").build();
  }

  @POST
  public Response stopRun() {
    // Return 'Not Implemented' status
    return Response.status(501).build();
  }
}
