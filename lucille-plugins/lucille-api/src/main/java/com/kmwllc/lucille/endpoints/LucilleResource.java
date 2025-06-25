package com.kmwllc.lucille.endpoints;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.kmwllc.lucille.AuthHandler;
import com.kmwllc.lucille.core.RunDetails;
import com.kmwllc.lucille.core.Runner;
import com.kmwllc.lucille.core.RunnerManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dropwizard.auth.Auth;
import io.dropwizard.auth.PrincipalImpl;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

/**
 * Lucille Admin API Endpoint.
 * <p>
 * Provides endpoints for managing Lucille configurations and triggering runs via the Admin API.
 * <ul>
 *   <li>Create and retrieve Lucille configs</li>
 *   <li>Start and retrieve Lucille runs</li>
 * </ul>
 */
@Path("/v1")
@Produces(MediaType.APPLICATION_JSON)
public class LucilleResource {

  /**
   * Logger for the LucilleResource.
   */
  private static final Logger log = LoggerFactory.getLogger(RunnerManager.class);

  /**
   * Runner manager instance for handling Lucille runs.
   */
  private final RunnerManager runnerManager;

  /**
   * Auth handler for authenticating requests.
   */
  private final AuthHandler authHandler;

  /**
   * Constructs a new LucilleResource.
   * @param runnerManager the runner manager instance
   * @param authHandler the authentication handler
   */
  public LucilleResource(RunnerManager runnerManager, AuthHandler authHandler) {
    this.runnerManager = runnerManager;
    this.authHandler = authHandler;
  }

  /**
   * Creates a new Lucille config, which can be used later by its referenced UUID.
   * @param user the authenticated user (optional)
   * @param configBody the config as a key-value map
   * @return HTTP 200 with config ID, or error if invalid
   */
  @POST
  @Tag(name = "Config")
  @Path("/config")
  @Consumes(MediaType.APPLICATION_JSON)
  @Operation(summary = "Create a new Lucille config to be run later",
      description = "Creates a new Lucille config, which can be used later by its referenced uuid.")
  public Response createConfig(@Parameter(hidden = true) @Auth Optional<PrincipalImpl> user,
      @RequestBody(description = "Run configuration as a key-value map", required = true,
          content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(
              type = "object",
              example = "{\"connectors\":[{\"class\":\"com.kmwllc.lucille.connector.CSVConnector\",\"path\":\"conf/dummy2.csv\",\"name\":\"connector1\",\"pipeline\":\"pipeline1\"}],\"pipelines\":[{\"name\":\"pipeline1\",\"stages\":[]}],\"indexer\":{\"type\":\"CSV\"},\"csv\":{\"columns\":[\"Name\",\"Age\",\"City\"],\"path\":\"conf/dummy.csv\",\"includeHeader\":false}}"))) Map<String, Object> configBody) {

    Response authResponse = authHandler.authenticate(user);
    if (authResponse != null) {
      return authResponse; // Return if authentication fails
    }

    try {
      Config config = ConfigFactory.parseMap(configBody);
      String configId = runnerManager.createConfig(config);
      log.info("a lucille config has been created. Config ID: " + configId);
      Map<String, Object> ret = new HashMap<>();
      ret.put("configId", configId);
      return Response.ok(ret).build();
    } catch (Exception e) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("Invalid configuration provided: " + e.getMessage()).build();
    }
  }

  /**
   * Retrieves all Lucille configurations.
   * @param user the authenticated user (optional)
   * @return HTTP 200 with all configs, or error if unauthorized
   */
  @GET
  @Tag(name = "Config", description = "Retrieve Lucille configuration details.")
  @Path("/config")
  @Operation(summary = "Get all Lucille configs",
      description = "Retrieves all Lucille configurations.",
      security = @SecurityRequirement(name = "basicAuth"))
  public Response getAllConfigs(@Parameter(hidden = true) @Auth Optional<PrincipalImpl> user) {
    Response authResponse = authHandler.authenticate(user);
    if (authResponse != null) {
      return authResponse; // Return if authentication fails
    }
    Map<String, Object> ret = new HashMap<>();
    for (String configId : runnerManager.getConfigKeys  ()) {
      ret.put(configId, runnerManager.getConfig(configId).root().unwrapped());
    }
    return Response.ok(ret).build();
  }

  /**
   * Retrieves a specific Lucille configuration by its ID.
   * @param user the authenticated user (optional)
   * @param configId the UUID of the configuration to retrieve
   * @return HTTP 200 with config details, 404 if not found, or error if unauthorized
   */
  @GET
  @Tag(name = "Config", description = "Retrieve Lucille configuration details.")
  @Path("/config/{configId}")
  @Operation(summary = "Get a specific Lucille config",
      description = "Retrieves a specific Lucille configuration by its ID.",
      security = @SecurityRequirement(name = "basicAuth"))
  public Response getConfig(@Parameter(hidden = true) @Auth Optional<PrincipalImpl> user,
      @Parameter(description = "The UUID of the configuration to retrieve.", required = false,
          example = "fca83cb6-c2c2-4cbf-93ef-41c08d5d4b58") @PathParam("configId") String configId) {
    Response authResponse = authHandler.authenticate(user);
    if (authResponse != null) {
      return authResponse; // Return if authentication fails
    }

    try {
      Config config = runnerManager.getConfig(configId);
      if (config == null) {
        return ResponseUtils.buildErrorResponse(Response.Status.NOT_FOUND,
            "Configuration with ID " + configId + " not found.");
      }
      return Response.ok(config.root().unwrapped()).build();
    } catch (Exception e) {
      return ResponseUtils.buildErrorResponse(Response.Status.INTERNAL_SERVER_ERROR,
          "Error retrieving configuration: " + e.getMessage());
    }
  }

  /**
   * Starts a new Lucille run with the specified configuration.
   * @param user the authenticated user (optional)
   * @param requestBody request body containing the configuration ID
   * @return HTTP 200 with run details, or error if invalid or unauthorized
   */
  @POST
  @Tag(name = "Run")
  @Path("/run")
  @Consumes(MediaType.APPLICATION_JSON)
  @Operation(summary = "Start a new Lucille run",
      description = "Triggers a new Lucille run with the specified configuration if none is currently running.")
  public Response startRun(@Parameter(hidden = true) @Auth Optional<PrincipalImpl> user,
      @RequestBody(description = "Request body containing the configuration ID.", required = true,
          content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(
              type = "object",
              example = "{\"configId\": \"550e8400-e29b-41d4-a716-446655440000\"}"))) Map<String, String> requestBody) {
    Response authResponse = authHandler.authenticate(user);
    if (authResponse != null) {
      return authResponse; // Return if authentication fails
    }

    try {
      // Extract configId from the request body
      String configId = requestBody.get("configId");
      if (configId == null || configId.isBlank()) {
        return ResponseUtils.buildErrorResponse(Response.Status.BAD_REQUEST,
            "configId is required in the request body.");
      }

      String runId = Runner.generateRunId();
      RunDetails details = runnerManager.runWithConfig(runId, configId);
      log.debug("Lucille run has been triggered. Run ID: " + runId);
      log.debug("details: {}", details);
      return Response.ok(details).build();
      
    } catch (Exception e) {
      return ResponseUtils.buildErrorResponse(Response.Status.BAD_REQUEST,
          "Invalid configuration provided: " + e.getMessage());
    }
  }

  /**
   * Retrieves a list of all Lucille runs.
   * @param user the authenticated user (optional)
   * @return HTTP 200 with all run details, or error if unauthorized
   */
  @GET
  @Tag(name = "Run", description = "Retrieve Lucille run details")
  @Path("/run")
  @Operation(summary = "Get all runs", description = "Retrieves a list of all Lucille runs.",
      security = @SecurityRequirement(name = "basicAuth"))
  public Response getAllRuns(@Parameter(hidden = true) @Auth Optional<PrincipalImpl> user) {
    Response authResponse = authHandler.authenticate(user);
    if (authResponse != null) {
      return authResponse;
    }

    return Response.ok(runnerManager.getRunDetails()).build();
  }

  /**
   * Retrieves the details of a specific Lucille run by its run ID.
   * @param user the authenticated user (optional)
   * @param runId the ID of the run to retrieve
   * @return HTTP 200 with run details, 400 if not found, or error if unauthorized
   */
  @GET
  @Tag(name = "Run", description = "Retrieve a specific Lucille run")
  @Path("/run/{runId}")
  @Operation(summary = "Get a specific run",
      description = "Retrieves the details of a specific Lucille run by its run ID.",
      security = @SecurityRequirement(name = "basicAuth"))
  public Response getRunById(@Parameter(hidden = true) @Auth Optional<PrincipalImpl> user,
      @Parameter(description = "The ID of the run to retrieve.", required = true,
          example = "550e8400-e29b-41d4-a716-446655440000") @PathParam("runId") String runId) {
    Response authResponse = authHandler.authenticate(user);
    if (authResponse != null) {
      return authResponse;
    }

    RunDetails details = runnerManager.getRunDetails(runId);
    if (details == null) {

      return ResponseUtils.buildErrorResponse(Response.Status.BAD_REQUEST,
          "No run with the given ID was found.");
    }

    return Response.ok(details).build();
  }

}
