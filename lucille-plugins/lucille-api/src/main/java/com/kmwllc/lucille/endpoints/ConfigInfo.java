package com.kmwllc.lucille.endpoints;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.connector.AbstractConnector;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.spec.Spec;

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
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.*;
import java.io.InputStream;
import java.io.IOException;
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

  private ArrayNode cachedConnectorListJson;
  private ArrayNode cachedStageListJson;
  private ArrayNode cachedIndexerListJson;

  public ConfigInfo(AuthHandler authHandler) {
    this.authHandler = authHandler;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ComponentDoc {
    public String className;
    public String description;
    public String javadoc;
  }

  // Extract the base description from the javadoc
  private static String extractDescriptionFromHtml(String html) {
    if (html == null || html.isEmpty()) {
      return null;
    }

    int idx  = html.toLowerCase().indexOf("<p>");
    String head = (idx >= 0) ? html.substring(0, idx) : html;

    return Jsoup.parse(head).text().trim();
  }

  // Extract the field descriptions from the javadoc
  private static Map<String, String> extractParamDescriptionsFromHtml(String html) {
    Map<String, String> out = new HashMap<>();

    // Return on empty input
    if (html == null || html.isEmpty()) {
      return out;
    }

    Document jsoupDoc = Jsoup.parse(html);
    // Select a <p> where the text matches "config parameters" and grab the immediate child <ul>
    Elements uls = jsoupDoc.select("p:matchesOwn((?i)^\\s*config\\s*parameters\\b) + ul");
    if (uls.isEmpty()) {
      return out;
    }

    // Iterate over each top level <li> in the <ul>
    for (org.jsoup.nodes.Element li : uls.first().select("> li")) {
      String text = li.text();
      int colon = text.indexOf(':');
      if (colon < 0) {
        continue;
      }

      // Extract the name and description from each <li>
      String rawName = text.substring(0, colon).trim();
      String description = text.substring(colon + 1).trim();

      // Format the name to match that of the spec exactly
      String baseName = rawName.replaceFirst("\\s*\\(.*\\)$", "").trim();
      if (!baseName.isEmpty()) {
        out.put(baseName, description);
      }
    }

    return out;
  }

  // Load the docs from the provided json file
  private static Map<String, ComponentDoc> loadDocs(String resourceName) throws IOException {
    Map<String, ComponentDoc> map = new HashMap<>();

    try (InputStream is = ConfigInfo.class.getClassLoader().getResourceAsStream(resourceName)) {
      if (is == null) {
        return map;
      }

      // Get the document stream as an array of ComponentDocs
      List<ComponentDoc> list = Arrays.asList(mapper.readValue(is, ComponentDoc[].class));

      // Put each ComponentDoc into the map with the name as the key
      for (ComponentDoc d : list) {
        map.put(d.className, d);
      }

      return map;
    }
  }

  // Merges in javadoc to description and spec fields
  private static void mergeJavadocIntoFields(ObjectNode specNode, String javadocHtml) {
    // TODO: Determine if we want this default paramsFromDescription removed
    specNode.remove("paramsFromDescription");

    if (javadocHtml == null) {
      return;
    }

    specNode.put("javadoc", javadocHtml);
    String shortDesc = extractDescriptionFromHtml(javadocHtml);

    if (shortDesc != null && !shortDesc.isEmpty()) {
      specNode.put("description", shortDesc);
    }

    Map<String, String> paramDescs = extractParamDescriptionsFromHtml(javadocHtml);
    JsonNode fieldsNode = specNode.get("fields");

    if (fieldsNode != null && fieldsNode.isArray()) {
      ArrayNode fieldsArray = (ArrayNode) fieldsNode;

      // Iterate over each field definition
      for (JsonNode fn : fieldsArray) {
        if (fn.isObject()) {
          ObjectNode fieldObj = (ObjectNode) fn;
          JsonNode nameNode = fieldObj.get("name");

          if (nameNode != null) {
            // If a matching field name is found in the html write it to the description
            String fieldName = nameNode.asText();
            String desc = paramDescs.get(fieldName);

            if (desc != null && !desc.isEmpty()) {
              fieldObj.put("description", desc);
            }
          }
        }
      }
    }
  }

  // Builds an array of specs for the subclasses of a base class
  private ArrayNode buildSpecArrayForSubclasses(String baseClassName, Map<String, ComponentDoc> docs)
      throws NoSuchFieldException, IllegalAccessException {
    ArrayNode array = mapper.createArrayNode();
    // Use ClassGraph to scan the classpath
    try (ScanResult scanResult = new ClassGraph().enableAllInfo().scan()) {
      // Find every class that is a subclass of the provided class name
      ClassInfoList classes = scanResult.getSubclasses(baseClassName);
      // Load whatever matches were found as class objects
      List<Class<?>> refs = classes.loadClasses();

      // Iterate over each subclass
      for (Class<?> c : refs) {
        // Get the specs
        ObjectNode specNode = (ObjectNode) ((Spec) c.getDeclaredField("SPEC").get(null)).toJson();
        specNode.put("class", c.getName());
        array.add(specNode);

        // If the class has javadoc descriptions, merge them in to the spec
        ComponentDoc d = docs.get(c.getName());
        mergeJavadocIntoFields(specNode, (d != null ? d.description: null));
      }
    }

    return array;
  }

  @GET
  @Path("/connector-list")
  public Response getConnectors(@Parameter(hidden = true) @Auth Optional<PrincipalImpl> user)
      throws IOException, NoSuchFieldException, IllegalAccessException {
    Response authResponse = authHandler.authenticate(user);
    if (authResponse != null) {
      return authResponse;
    }

    if (cachedConnectorListJson == null) {
      Map<String, ComponentDoc> connectorDocs = loadDocs("connector-javadocs.json");
      cachedConnectorListJson = buildSpecArrayForSubclasses(AbstractConnector.class.getName(), connectorDocs);
    }

    return Response.ok(cachedConnectorListJson, MediaType.APPLICATION_JSON).build();
  }

  @GET
  @Path("/stage-list")
  public Response getStages(@Parameter(hidden = true) @Auth Optional<PrincipalImpl> user)
      throws IOException, NoSuchFieldException, IllegalAccessException {
    Response authResponse = authHandler.authenticate(user);
    if (authResponse != null) {
      return authResponse;
    }

    if (cachedStageListJson == null) {
      Map<String, ComponentDoc> stageDocs = loadDocs("stage-javadocs.json");
      cachedStageListJson = buildSpecArrayForSubclasses(Stage.class.getName(), stageDocs);
    }

    return Response.ok(cachedStageListJson, MediaType.APPLICATION_JSON).build();
  }

  @GET
  @Path("/indexer-list")
  public Response getIndexers(@Parameter(hidden = true) @Auth Optional<PrincipalImpl> user)
      throws IOException, NoSuchFieldException, IllegalAccessException {
    Response authResponse = authHandler.authenticate(user);
    if (authResponse != null) {
      return authResponse;
    }

    if (cachedIndexerListJson == null) {
      Map<String, ComponentDoc> indexerDocs = loadDocs("indexer-javadocs.json");
      cachedIndexerListJson = buildSpecArrayForSubclasses(Indexer.class.getName(), indexerDocs);
    }

    return Response.ok(cachedIndexerListJson, MediaType.APPLICATION_JSON).build();
  }
}