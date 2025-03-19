package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A stage for running a search request on an Opensearch index, specifying a field in the response, and putting that field's value
 * on a Document. The specified query can either be found in a Document's field or specified in your Config.
 *
 * opensearch (Map): Configuration for your Opensearch instance. Should contain the url to your Opensearch instance, the index
 * name you want to query on, and whether invalid certificates can be accepted. See the opensearch ingest example for an
 * example of the configuration needed. (Be sure to include it as part of the Stage's Config.)
 *
 * templateName (String, Optional): The name / id of a saved search template in your Opensearch cluster that you want to use. If not specified,
 * you must specify a searchTemplate to use for the Stage instead.
 *
 * searchTemplate (String, Optional): The query template you want to use. The parameter names you define should match field names
 * in the Documents you are processing. You can define default values if fields will not always be prevalent in Documents. See
 * Opensearch's Search Templates documentation for information about defining templates, parameters, default values, etc. If not
 * specified, you must specify a templateName to use for the Stage instead. This template will <b>not</b> be saved to your Opensearch
 * cluster.
 *
 * requiredParamNames (list of String, optional): A list of field names that you require to be found on every document. This list should...
 *   1.) Contain all parameters you defined in your search template that <b>do not</b> have default values
 *   2.) Be made of names that can be found on Documents.
 * If any of the required parameters are missing, Lucille will log a warning and put an error in "queryOpensearchError" on the Document.
 *
 * optionalParamNames (list of String, optional): A list of field names that you do not require to be found on every document, but want
 * to include in your if they are present. This list should...
 *   1.) contain all parameters in your search template that have default values
 *   2.) Be made of names that can be found on Documents.
 * If any of the optional parameters are missing, they will not be used in the search, and the default value will be used (by Opensearch) instead.
 *
 * <b>NOTE:</b> If the field names on your Document do not match the parameter names in your search template, use the
 * <b>RenameFields</b> stage to match them.
 *
 * <b>NOTE:</b> If a field without a default value is missing, Opensearch does not throw an Exception. Instead, it returns a response with 0 hits,
 * which can cause in undesirable results or potential JSON-related exceptions. As such, it is important to define <b>requiredParamNames</b>
 * and <b>optionalParamNames</b> very carefully!
 *
 * opensearchResponsePath (String, Optional): A path to a field in the Opensearch response whose value you want to place on a Lucille Document.
 * Use JsonPointer notation (separating fields / array indices by a '/'). An IllegalArgumentException will be thrown if the path has invalid formatting.
 * Defaults to using the entire response.
 *
 * destinationField (String, Optional): The name of the field you'll write the response value to in a Lucille Document. Defaults to "response".
 */
public class QueryOpensearch extends Stage {

  private static final Logger log = LoggerFactory.getLogger(QueryOpensearch.class);

  private final URI searchURI;

  private final String templateName;
  private final String searchTemplateStr;
  private final List<String> requiredParamNames;
  private final List<String> optionalParamNames;
  private final JsonPointer opensearchResponsePath;
  private final String destinationField;

  private final HttpClient httpClient;

  private JsonNode searchTemplateJson;

  public QueryOpensearch(Config config) {
    super(config, new StageSpec()
        .withRequiredParents("opensearch")
        .withOptionalProperties("templateName", "searchTemplate", "requiredParamNames",
            "optionalParamNames", "opensearchResponsePath", "destinationField"));

    this.searchURI = getTemplateSearchURI(config);

    this.templateName = ConfigUtils.getOrDefault(config, "templateName", null);
    this.searchTemplateStr = ConfigUtils.getOrDefault(config, "searchTemplate", null);

    // XNOR - cannot both be null, cannot both be specified.
    if ((templateName == null) == (searchTemplateStr == null)) {
      throw new IllegalArgumentException("You must specify templateName or searchTemplate.");
    }

    this.requiredParamNames = ConfigUtils.getOrDefault(config, "requiredParamNames", List.of());
    this.optionalParamNames = ConfigUtils.getOrDefault(config, "optionalParamNames", List.of());

    if (config.hasPath("opensearchResponsePath")) {
      this.opensearchResponsePath = JsonPointer.compile(config.getString("opensearchResponsePath"));
    } else {
      this.opensearchResponsePath = JsonPointer.empty();
    }

    this.destinationField = ConfigUtils.getOrDefault(config, "destinationField", "response");

    httpClient = HttpClient.newHttpClient();
  }

  @Override
  public void start() throws StageException {
    // Get JSON for the search template, if it was specified.
    if (searchTemplateStr == null) {
      return;
    }

    try {
      ObjectMapper mapper = new ObjectMapper();
      searchTemplateJson = mapper.readTree(searchTemplateStr);
    } catch (JsonProcessingException e) {
      throw new StageException("Error building JSON from the searchTemplate provided.", e);
    }
  }

  // TODO: If we require Java 21 we could add a call to httpClient.close().

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    ObjectNode requestJson;

    if (searchTemplateJson != null) {
      // Use an existing search template. Need the json for the template, will populate it with "params" for this document.
      requestJson = (ObjectNode) searchTemplateJson;
    } else {
      // 2. Lookup by an existing template. Populate the "id" field. Will populate it with "params" for this document.
      ObjectMapper mapper = new ObjectMapper();
      requestJson = mapper.createObjectNode().put("id", templateName);
    }

    // If there is an exception (a missing required params?) then log a warning, put an error, and return.
    try {
      populateJsonWithParams(doc, requestJson);
    } catch (NoSuchFieldException e) {
      log.warn("A required field was missing from Document, no query will be executed.", e);
      doc.setField("queryOpensearchError", "A requiredField was missing.");
      return null;
    }

    // Building the actual request and then executing it
    HttpRequest httpRequest = HttpRequest.newBuilder()
        .uri(searchURI)
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(requestJson.toString()))
        .build();

    try {
      HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());

      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode currentNode = objectMapper.readTree(response.body());

      JsonNode responseFieldNode = currentNode.at(opensearchResponsePath);
      doc.setField(destinationField, responseFieldNode.toString());
    } catch (Exception e) {
      throw new StageException("Error occurred executing the Opensearch Query + Getting Response.", e);
    }

    return null;
  }

  /**
   * Adds a "params" node to the root of the given JSON, populated with the required / optional param names present on the given Document.
   * Throws a NoSuchFieldException if a "requiredParam" is not present on the given document.
   * (package access so we can test, make sure we are getting the appropriate objects)
   */
  void populateJsonWithParams(Document doc, ObjectNode json) throws NoSuchFieldException {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode paramsNode = objectMapper.createObjectNode();

    for (String requiredParamName : requiredParamNames) {
      if (!doc.has(requiredParamName)) {
        throw new NoSuchFieldException("Field " + requiredParamName + " is required, but missing from Document " + doc.getId() + ".");
      }

      // OpenSearch can handle parsing doubles / ints from a String when that is the parameter type needed.
      paramsNode.put(requiredParamName, doc.getString(requiredParamName));
    }

    for (String optionalParamName : optionalParamNames) {
      if (doc.has(optionalParamName)) {
        paramsNode.put(optionalParamName, doc.getString(optionalParamName));
      }
    }

    json.set("params", paramsNode);
  }

  /**
   * A URI to the search endpoint on Opensearch based on the given config.
   */
  URI getTemplateSearchURI(Config config) {
    String uriString = config.getString("opensearch.url") + "/" + config.getString("opensearch.index") + "/_search/template";
    return URI.create(uriString);
  }
}
