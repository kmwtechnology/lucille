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
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.*;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

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

}