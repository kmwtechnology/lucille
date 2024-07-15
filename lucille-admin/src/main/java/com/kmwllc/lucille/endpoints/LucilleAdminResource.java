package com.kmwllc.lucille.endpoints;

import com.kmwllc.lucille.core.RunnerManager;
import com.kmwllc.lucille.objects.RunStatus;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;

import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/admin")
@Produces(MediaType.APPLICATION_JSON)
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
  public Response startRun() {
    rm.run();

    return Response.ok("Lucille run has been triggered.").build();
  }
}
