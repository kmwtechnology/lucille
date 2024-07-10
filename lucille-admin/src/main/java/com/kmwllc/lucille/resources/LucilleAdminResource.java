package com.kmwllc.lucille.resources;

import com.kmwllc.lucille.core.RunnerManager;
import com.kmwllc.lucille.objects.RunStatus;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;

import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

public class LucilleAdminResource {

  private final RunnerManager rm;

  public LucilleAdminResource() {
    rm = RunnerManager.getInstance();
  }

  @GET
  public Response getRunStatus() {
    boolean isRunning = rm.isRunning();

    RunStatus runStatus = new RunStatus(isRunning);

    return Response.ok(runStatus).build();
  }

  @POST
  public Response startRun( @PathParam("local") boolean local) {
    rm.run(local);

    // TODO : Should we alert if the run was skipped?
    return Response.ok("Lucille run has been triggered.").build();
  }

  @POST
  public Response stopRun() {
    // Return 'Not Implemented' status
    return Response.status(501).build();
  }
}
