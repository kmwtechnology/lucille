package com.kmwllc.lucille.endpoints;

import com.kmwllc.lucille.core.RunnerManager;
import com.kmwllc.lucille.objects.RunStatus;
import io.dropwizard.auth.Auth;
import io.dropwizard.auth.PrincipalImpl;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;

import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Optional;

/**
 * Lucille Admin API Endpoint:
 *
 * - endpoint: '/lucille'
 *   - GET: obtain lucille run status
 *   - POST: Kick off new lucille run, if one is not already running
 */
@Path("/lucille")
@Produces(MediaType.APPLICATION_JSON)
public class LucilleResource {

  private final RunnerManager runnerManager;

  public LucilleResource(RunnerManager runnerManager) {
    this.runnerManager = runnerManager;
  }

  @GET
  public Response getRunStatus(@Auth Optional<PrincipalImpl> user) {
    if (user.isPresent()) {
      boolean isRunning = runnerManager.isRunning();

      RunStatus runStatus = new RunStatus(isRunning);

      return Response.ok(runStatus).build();
    } else {
      return Response.status(401).build();
    }
  }

  @POST
  public Response startRun(@Auth Optional<PrincipalImpl> user) {

    if (user.isPresent()) {
      boolean status = runnerManager.run();

      if (status) {
        return Response.ok("Lucille run has been triggered.").build();
      } else {
        return Response.status(424).entity("This lucille run has been skipped. An instance of lucille is already running.").build();
      }
    } else {
      return Response.status(401).build();
    }
  }
}
