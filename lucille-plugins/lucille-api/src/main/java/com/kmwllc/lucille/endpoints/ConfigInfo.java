package com.kmwllc.lucille.endpoints;

import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.util.ClassUtils;
import com.kmwllc.lucille.util.ClassUtils.ClassDescriptor;

import io.dropwizard.auth.Auth;
import io.dropwizard.auth.PrincipalImpl;
import com.kmwllc.lucille.AuthHandler;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.*;
import java.io.InputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * API endpoint to provide available connectors, stages, and indexers,
 * including their configuration parameters and type information.
 */
@Path("/v1/config-info")
@Tag(name = "Config")
@Produces(MediaType.APPLICATION_JSON)
public class ConfigInfo {

  private final AuthHandler authHandler;

  public ConfigInfo(AuthHandler authHandler) {
    this.authHandler = authHandler;
  }

  /**
   * Scans the given package for classes and extracts config parameter info.
   * This is a placeholder implementation; in a real system, you would use a classpath scanner
   * like Reflections or a registry of known components.
   * @throws ClassNotFoundException 
   */
  private Set<ClassDescriptor>getComponentInfo(String parentClassName, String packageName) throws ClassNotFoundException {

    Set<ClassDescriptor> components = ClassUtils.findSubclassDescriptors(parentClassName, packageName);
    
    return components;
  }


  /**
   * Returns all available connectors with their config parameters.
   * @throws ClassNotFoundException 
   */
  @GET
  @Path("/connector-list")
  public Response getConnectors(@Parameter(hidden = true) @Auth Optional<PrincipalImpl> user) throws ClassNotFoundException {
    Response authResponse = authHandler.authenticate(user);
    if (authResponse != null) {
      return authResponse;
    }
    Set<ClassDescriptor> connectors = ClassUtils.findInterfaceDescriptors(Connector.class.getName(), "com.kmwllc.lucille.connector");
    return Response.ok(connectors).build();
  }

  /**
   * Returns all available stages with their config parameters.
   * @throws ClassNotFoundException 
   */
  @GET
  @Path("/stage-list")
  public Response getStages(@Parameter(hidden = true) @Auth Optional<PrincipalImpl> user) throws ClassNotFoundException {
    Response authResponse = authHandler.authenticate(user);
    if (authResponse != null) {
      return authResponse;
    }
    Set<ClassDescriptor> stages = getComponentInfo(Stage.class.getName(), "com.kmwllc.lucille.stage");
    return Response.ok(stages).build();
  }

  /**
   * Returns all available indexers with their config parameters.
   * @throws ClassNotFoundException 
   */
  @GET
  @Path("/indexer-list")
  public Response getIndexers(@Parameter(hidden = true) @Auth Optional<PrincipalImpl> user) throws ClassNotFoundException {
    Response authResponse = authHandler.authenticate(user);
    if (authResponse != null) {
      return authResponse;
    }
    Set<ClassDescriptor> indexers = getComponentInfo(Indexer.class.getName(), "com.kmwllc.lucille.indexer");
    return Response.ok(indexers).build();
  }

  /**
   * Returns detailed javadoc information for the specified component type.
   * Loads the information from a JSON file in the resources directory named {componentType}-javadocs.json.
   * 
   * @param componentType The type of component to get javadocs for. Valid values are "stage", "connector", or "indexer".
   * @param user The authenticated user
   * @return The javadoc information as a JSON response
   */
  @GET
  @Path("/javadoc-list/{componentType}")
  public Response getJavadocs(
      @Parameter(description = "Component type to get javadocs for", required = true) 
      @PathParam("componentType") String componentType,
      @Parameter(hidden = true) @Auth Optional<PrincipalImpl> user) {
    
    Response authResponse = authHandler.authenticate(user);
    if (authResponse != null) {
      return authResponse;
    }
    
    // Validate component type
    if (!Arrays.asList("stage", "connector", "indexer").contains(componentType.toLowerCase())) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("Invalid component type. Must be one of: stage, connector, indexer")
          .build();
    }
    
    String resourcePath = componentType.toLowerCase() + "-javadocs.json";
    
    try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
      if (is == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity("Javadoc information not found for component type: " + componentType)
            .build();
      }
      
      String jsonContent = new String(is.readAllBytes(), StandardCharsets.UTF_8);
      return Response.ok(jsonContent, MediaType.APPLICATION_JSON).build();
      
    } catch (IOException e) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("Error reading javadoc information: " + e.getMessage())
          .build();
    }
  }

}