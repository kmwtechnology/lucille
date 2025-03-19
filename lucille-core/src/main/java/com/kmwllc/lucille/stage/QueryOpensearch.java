package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.JsonPointer;
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

/**
 * A stage for running a search request on an Opensearch index, specifying a field in the response, and putting that field's value
 * on a Document. The specified query can either be found in a Document's field or specified in your Config.
 *
 * opensearch (Map): Configuration for your Opensearch instance. Should contain the url to your Opensearch instance, the index
 * name you want to query on, and whether invalid certificates can be accepted. See the opensearch ingest example for an
 * example of the configuration needed. (Be sure to include it as part of the Stage's Config.)
 *
 * templateName (String): The name you want to associate your search template with.
 *
 * searchTemplate (String): A query template to provide to Opensearch. The parameter names you define should match field names
 * in the Documents you are processing. You can define default values if fields will not always be prevalent in Documents. See
 * Opensearch's Search Templates documentation for information about defining templates, parameters, default values, etc.
 *
 * paramNames (list of String): A list of field names. This list should...
 *   1.) Contain all of the parameter names you defined in your search template
 *   2.) Be composed of field names that will be found in the Documents you process.
 * It is important to note that Exceptions are not thrown when a parameter without a default value is not found in your Document and,
 * as such, not specified in the template's execution. Rather, Opensearch returns an empty response.
 *
 *
 * TODO: Is that needed?
 * requiredParamNames (list of String, optional): A list of field names that you require to be found on every document. This list should...
 *   1.) Contain all parameters you defined in your search template that do <b>not</b> have default values
 *   2.) Be found on every Document you intend to process.
 * If any of the required parameters are missing, Lucille will throw an Exception to prevent unexpected query results from being applied to Documents.
 *
 * optionalParamNames (list of String, optional): A list of field names that you do not require to be found on every document, but want
 * to include in your query if they are found. This list should...
 *   1.) contain all parameters you defined in your search template that have default values
 *   2.) The same field names should be found on Documents (when present / defined).
 * If any of the optional parameters are missing, they will not be used in the search, and the default value will be used instead.
 *
 * opensearchResponsePath (String, Optional): A path to a field in the Opensearch response whose value you want to place on a Lucille Document.
 * Use JsonPointer notation. An IllegalArgumentException will be thrown if the path has invalid formatting. Defaults to using the entire
 * response.
 *
 * destinationField (String, Optional): The name of the field you'll write the response value to in a Lucille Document. Defaults to "response".
 */
public class QueryOpensearch extends Stage {

  private final URI searchURI;
  private final URI templateURI;

  private final String templateName;
  private final String searchTemplate;
  private final List<String> paramNames;
  private final JsonPointer opensearchResponsePath;
  private final String destinationField;

  private final HttpClient httpClient;

  public QueryOpensearch(Config config) {
    super(config, new StageSpec()
        .withRequiredParents("opensearch")
        .withRequiredProperties("templateName", "searchTemplate", "paramNames")
        .withOptionalProperties("opensearchResponsePath", "destinationField"));

    this.searchURI = getSearchURI(config);
    this.templateURI = getTemplateURI(config);

    this.templateName = config.getString("templateName");
    this.searchTemplate = config.getString("searchTemplate");
    this.paramNames = config.getStringList("paramNames");

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
    // Register the search template with Opensearch.
    HttpRequest request = HttpRequest.newBuilder()
        .uri(templateURI)
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(searchTemplate))
        .build();

    try {
      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200) {
        throw new StageException("Non-200 status code from registering Search Template with Opensearch, response: " + response.body());
      }
    } catch (Exception e) {
      throw new StageException("Error occurred executing the Opensearch Query.", e);
    }
  }

  // TODO: If we require Java 21 we could add a call to httpClient.close().

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    // Build a JSON object - fill out "id" and "params". Pretty simple... will use the list of Strings
    JsonNode templateForQuery = jsonSearchForDoc(doc);

    HttpRequest request = HttpRequest.newBuilder()
        .uri(searchURI)
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(templateForQuery.toString()))
        .build();

    try {
      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode currentNode = objectMapper.readTree(response.body());

      JsonNode responseFieldNode = currentNode.at(opensearchResponsePath);
      doc.setField(destinationField, responseFieldNode.toString());
    } catch (Exception e) {
      throw new StageException("Error occurred executing the Opensearch Query.", e);
    }

    return null;
  }

  // Creates a search request that will invoke the template with the given templateName, filling out the
  // "params" using all
  private JsonNode jsonSearchForDoc(Document doc) {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode request = objectMapper.createObjectNode();

    request.put("id", templateName);

    ObjectNode paramsNode = objectMapper.createObjectNode();

    for (String paramName : paramNames) {
      // OpenSearch can handle parsing doubles / ints from a String when that is the parameter type needed.
      paramsNode.put(paramName, doc.getString(paramName));
    }

    request.set("params", paramsNode);
    return request;
  }

  /**
   * A URI to the search template endpoint on Opensearch, based on the given config.
   */
  private URI getTemplateURI(Config config) {
    String uriString = config.getString("opensearch.url") + "/_scripts/" + config.getString("templateName");
    return URI.create(uriString);
  }

  /**
   * A URI to the search endpoint on Opensearch based on the given config.
   */
  private URI getSearchURI(Config config) {
    String uriString = config.getString("opensearch.url") + "/" + config.getString("opensearch.index") + "/_search/template";
    return URI.create(uriString);
  }
}
