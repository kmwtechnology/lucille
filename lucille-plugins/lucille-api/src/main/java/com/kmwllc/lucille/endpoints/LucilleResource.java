package com.kmwllc.lucille.endpoints;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.kmwllc.lucille.core.RunDetails;
import com.kmwllc.lucille.core.RunnerManager;
import com.kmwllc.lucille.objects.RunStatus;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.dropwizard.auth.Auth;
import io.dropwizard.auth.PrincipalImpl;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import jakarta.ws.rs.Consumes;
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

  private static final Logger log = LoggerFactory.getLogger(RunnerManager.class);
    
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
  @Consumes(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Start a new Lucille run", 
      description = "Triggers a new Lucille run with the specified configuration if none is currently running."
  )
  public Response startRun(
      @Parameter(hidden = true) @Auth Optional<PrincipalImpl> user,
      Map<String, Object> configBody) { // Receive the request body as a Map
      if (user.isPresent()) {
          try {
              // Convert the request body into a Config object
              Config config = ConfigFactory.parseMap(configBody);

              String runId = UUID.randomUUID().toString();
              boolean status = runnerManager.runWithConfig(runId, config);
              RunDetails details = runnerManager.getRunDetails(runId);
              log.info("Lucille run has been triggered. Run ID: " + runId);
              log.info("details: {}", details);

              if (status) {
                  return Response.ok("Lucille run has been triggered. Run ID: " + runId).build();
              } else {
                  return Response.status(424)
                      .entity("This Lucille run has been skipped. An instance of Lucille is already running.")
                      .build();
              }
          } catch (Exception e) {
              return Response.status(Response.Status.BAD_REQUEST)
                  .entity("Invalid configuration provided: " + e.getMessage())
                  .build();
          }
      } else {
          return Response.status(Response.Status.UNAUTHORIZED).build();
      }
  }
  
}
