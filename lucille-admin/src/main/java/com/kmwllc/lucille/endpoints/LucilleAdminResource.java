package com.kmwllc.lucille.endpoints;

import com.kmwllc.lucille.core.RunnerManager;
import com.kmwllc.lucille.objects.RunStatus;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;

import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

/**
 * Lucille Admin API Endpoint:
 *
 * - endpoint: '/admin'
 *   - GET: obtain lucille run status
 *   - POST: Kick off new lucille run, if one is not already running
 */
@Path("/admin")
@Produces(MediaType.APPLICATION_JSON)
public class LucilleAdminResource {

  private final RunnerManager runnerManager;

  public LucilleAdminResource(RunnerManager runnerManager) {
    this.runnerManager = runnerManager;
  }

  @GET
  public Response getRunStatus() {
    boolean isRunning = runnerManager.isRunning();

    RunStatus runStatus = new RunStatus(isRunning);

    return Response.ok(runStatus).build();
  }

  @POST
  public Response startRun() {
    boolean status = runnerManager.run();

    if (status) {
      return Response.ok("Lucille run has been triggered.").build();
    } else {
      return Response.ok("This lucille run has been skipped. An instance of lucille is already running.").build();
    }
  }
}
