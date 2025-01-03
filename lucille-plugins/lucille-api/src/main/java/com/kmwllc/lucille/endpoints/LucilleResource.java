package com.kmwllc.lucille.endpoints;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kmwllc.lucille.AuthHandler;
import com.kmwllc.lucille.core.RunDetails;
import com.kmwllc.lucille.core.RunnerManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.dropwizard.auth.Auth;
import io.dropwizard.auth.PrincipalImpl;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
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
 * - endpoint: '/lucille' - GET: obtain lucille run status - POST: Kick off new
 * lucille run, if one is not already running
 */
@Tag(name = "Jobs", description = "Operations related to managing Lucille running jobs.")
@Path("/v1")
@Produces(MediaType.APPLICATION_JSON)
public class LucilleResource {

	private static final Logger log = LoggerFactory.getLogger(RunnerManager.class);

	private final RunnerManager runnerManager;

	private final AuthHandler authHandler;

	public LucilleResource(RunnerManager runnerManager, AuthHandler authHandler) {
		this.runnerManager = runnerManager;
		this.authHandler = authHandler;
	}

	@GET
	@Path("/get-status")
	@Operation(summary = "Get the run status", description = "Returns the current running status of the runner.", security = @SecurityRequirement(name = "basicAuth"))
	public Response getRunStatus(@Parameter(hidden = true) @Auth Optional<PrincipalImpl> user,
			@Parameter(description = "The ID of the run to check the status for", required = true) @QueryParam("runId") String runId) {

		Response authResponse = authHandler.authenticate(user);
		if (authResponse != null) {
			return authResponse;
		}

		// Validate the runId parameter
		if (runId == null || runId.isBlank()) {
			return Response.status(Response.Status.BAD_REQUEST).entity("The 'runId' parameter is required.").build();
		}

		RunDetails details = runnerManager.getStatus(runId);
		return Response.ok(details).build();
	}
	
	@GET
	@Path("/get-status-list")
	@Operation(summary = "Get the run status", description = "Returns the current running status of the runner.", security = @SecurityRequirement(name = "basicAuth"))
	public Response getStatusList(@Parameter(hidden = true) @Auth Optional<PrincipalImpl> user) {

		Response authResponse = authHandler.authenticate(user);
		if (authResponse != null) {
			return authResponse; // Return if authentication fails
		}

		List<RunDetails> list = runnerManager.getStatusList();
		return Response.ok(list).build();
	}


	@POST
	@Path("/start-run")
	@Consumes(MediaType.APPLICATION_JSON)
	@Operation(summary = "Start a new Lucille run", description = "Triggers a new Lucille run with the specified configuration if none is currently running.")
	public Response startRun(@Parameter(hidden = true) @Auth Optional<PrincipalImpl> user,
			Map<String, Object> configBody) {

		Response authResponse = authHandler.authenticate(user);
		if (authResponse != null) {
			return authResponse; // Return if authentication fails
		}

		try {

			Config config = ConfigFactory.parseMap(configBody);

			String runId = UUID.randomUUID().toString();
			boolean status = runnerManager.runWithConfig(runId, config);
			RunDetails details = runnerManager.getStatus(runId);
			log.info("Lucille run has been triggered. Run ID: " + runId);
			log.info("details: {}", details);

			if (status) {
				return Response.ok(details).build();
			} else {
				return Response.status(424)
						.entity("This Lucille run has been skipped. An instance of Lucille is already running.")
						.build();
			}
		} catch (Exception e) {
			return Response.status(Response.Status.BAD_REQUEST)
					.entity("Invalid configuration provided: " + e.getMessage()).build();
		}
	}

}
