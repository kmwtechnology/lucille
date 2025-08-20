package com.kmwllc.lucille.endpoints;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.util.ClassUtils;
import com.kmwllc.lucille.util.ClassUtils.ClassDescriptor;

import io.dropwizard.auth.Auth;
import io.dropwizard.auth.PrincipalImpl;
import com.kmwllc.lucille.AuthHandler;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
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
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

/**
 * API endpoint to provide available connectors, stages, and indexers,
 * including their configuration parameters and type information.
 */
@Path("/v1/config-info")
@Tag(name = "Config")
@Produces(MediaType.APPLICATION_JSON)
public class ConfigInfo {

  private static final ObjectMapper mapper = new ObjectMapper();
  private final AuthHandler authHandler;
  private String cachedStageListJson;

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
  public Response getStages(@Parameter(hidden = true) @Auth Optional<PrincipalImpl> user)
      throws ClassNotFoundException, IOException, NoSuchFieldException, IllegalAccessException {
    Response authResponse = authHandler.authenticate(user);
    if (authResponse != null) {
      return authResponse;
    }

    if (cachedStageListJson != null) {
      return Response.ok(cachedStageListJson, MediaType.APPLICATION_JSON).build();
    }

    List<StageDoc> stageDocList =
        Arrays.asList(mapper.readValue(ConfigInfo.class.getClassLoader().getResourceAsStream("stage-javadocs.json"),
            StageDoc[].class));

    Map<String,StageDoc> stageDocs = new HashMap<>();
    for (StageDoc doc : stageDocList) {
      stageDocs.put(doc.className, doc);
    }

    ArrayNode stageSpecArray = mapper.createArrayNode();
    try (ScanResult scanResult = new ClassGraph().enableAllInfo().scan()) {
      ClassInfoList stageClasses = scanResult.getSubclasses("com.kmwllc.lucille.core.Stage");
      List<Class<?>> stageClassRefs = stageClasses.loadClasses();

      for (Class stageClass: stageClassRefs) {
        ObjectNode specNode = (ObjectNode) ((Spec)stageClass.getDeclaredField("SPEC").get(null)).toJson();

        specNode.put("class", stageClass.getName());
        stageSpecArray.add(specNode);

        StageDoc doc = stageDocs.get(stageClass.getName());
        if (doc != null && doc.description != null) {
          specNode.put("description", doc.description);

          Document jsoupDoc = Jsoup.parse(doc.description);
          Elements uls = jsoupDoc.select("p:matchesOwn((?i)^\\s*config\\s*parameters\\b) + ul");
          if (!uls.isEmpty()) {
            ArrayNode jsonParams = mapper.createArrayNode();

            for (org.jsoup.nodes.Element li : uls.first().select("> li")) {
              String text = li.text();
              int colon = text.indexOf(':');
              if (colon < 0) {
                continue;
              }

              String name = text.substring(0, colon).trim();
              String description = text.substring(colon + 1).trim();

              ObjectNode jsonParam = mapper.createObjectNode();
              jsonParam.put("name", name);
              jsonParam.put("description", description);
              jsonParams.add(jsonParam);
            }
            specNode.set("paramsFromDescription", jsonParams);
          }
        }
      }
    }

    cachedStageListJson = mapper.writeValueAsString(stageSpecArray);
    return Response.ok(stageSpecArray, MediaType.APPLICATION_JSON).build();
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class StageDoc {
    public String className;
    public String description;
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

  public static void main(String[] args) throws ClassNotFoundException, IOException, NoSuchFieldException, IllegalAccessException {
    AuthHandler authHandler = new AuthHandler(false);

    ConfigInfo api = new ConfigInfo(authHandler);


    System.out.println(api.getStages(Optional.empty()).getEntity());
  }

}