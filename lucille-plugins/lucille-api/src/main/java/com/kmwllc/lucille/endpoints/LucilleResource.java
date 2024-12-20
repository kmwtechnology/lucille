package com.kmwllc.lucille.endpoints;

import java.util.Optional;
import java.util.UUID;

import com.kmwllc.lucille.core.RunnerManager;
import com.kmwllc.lucille.objects.RunStatus;

import io.dropwizard.auth.Auth;
import io.dropwizard.auth.PrincipalImpl;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

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
  @Operation(
      summary = "Get the run status", 
      description = "Returns the current running status of the runner.",
      security = @SecurityRequirement(name = "basicAuth")
  )
  public Response getRunStatus(
      @Parameter(hidden = true) @Auth Optional<PrincipalImpl> user,
      @Parameter(description = "The ID of the run to check the status for", required = true) 
      @QueryParam("runId") String runId) {
      
      if (user.isPresent()) {
          if (runId == null || runId.isEmpty()) {
              return Response.status(Response.Status.BAD_REQUEST).entity("jobId is required").build();
          }

          boolean isRunning = runnerManager.isRunning(runId);
          RunStatus runStatus = new RunStatus(isRunning);
          return Response.ok(runStatus).build();
      } else {
          return Response.status(Response.Status.UNAUTHORIZED).build();
      }
  }
  
  
  @POST
  @Operation(summary = "Start a new Lucille run", 
            description = "Triggers a new Lucille run if none is currently running.")
  public Response startRun(
      @Parameter(hidden = true) @Auth Optional<PrincipalImpl> user) {
    if (user.isPresent()) {
      String runId = UUID.randomUUID().toString();
      boolean status = runnerManager.run(runId);

      if (status) {
        return Response.ok("Lucille run has been triggered.").build();
      } else {
        return Response.status(424)
            .entity("This Lucille run has been skipped. An instance of Lucille is already running.")
            .build();
      }
    } else {
      return Response.status(Response.Status.UNAUTHORIZED).build();
    }
  }
}
