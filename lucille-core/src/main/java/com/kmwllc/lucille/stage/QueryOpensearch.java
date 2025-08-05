package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.BaseConfigException;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.OpenSearchUtils;
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

  public static final Spec SPEC = Spec.stage()
      .requiredParentWithDescription(OpenSearchUtils.OPENSEARCH_PARENT_SPEC, "Configuration for your OpenSearch instance.")
      .optionalString("templateName", "searchTemplate", "opensearchResponsePath", "destinationField")
      .optionalList("requiredParamNames", new TypeReference<List<String>>(){})
      .optionalList("optionalParamNames", new TypeReference<List<String>>(){});

  private static final Logger log = LoggerFactory.getLogger(QueryOpensearch.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  /**
   * Configuration class for the QueryOpensearch stage.
   * This class encapsulates all the settings that can be configured for the QueryOpensearch stage,
   * providing type safety, centralized validation, and clear documentation for each parameter.
   */
  public static class QueryOpensearchConfig extends BaseStageConfig {
    /**
     * The base URL of the OpenSearch instance (e.g., "http://localhost:9200").
     * This is a required parameter and must be specified in the 'opensearch.url' path in the configuration.
     */
    private String opensearchUrl;
    /**
     * The name of the OpenSearch index to query.
     * This is a required parameter and must be specified in the 'opensearch.index' path in the configuration.
     */
    private String opensearchIndex;
    /**
     * The name/ID of a pre-existing search template stored in the OpenSearch cluster.
     * If specified, {@code searchTemplateStr} must not be specified.
     * Optional: If not provided, {@code searchTemplateStr} must be provided.
     */
    private String templateName;
    /**
     * A string representation of the OpenSearch query template to use.
     * The parameter names defined in this template should match field names in the processed Documents.
     * This template will <b>not</b> be saved to the OpenSearch cluster.
     * If specified, {@code templateName} must not be specified.
     * Optional: If not provided, {@code templateName} must be provided.
     */
    private String searchTemplateStr;
    /**
     * A list of field names from the input Document that are required for the search template.
     * If any of these fields are missing from a Document, the query will not be executed for that Document,
     * and an error will be logged and recorded on the Document.
     * This list should typically include all parameters in your search template that do not have default values.
     * Defaults to an empty list.
     */
    private List<String> requiredParamNames = List.of();
    /**
     * A list of field names from the input Document that are optional for the search template.
     * If these fields are present in a Document, their values will be used as parameters in the search query.
     * If they are missing, OpenSearch will use the default values defined in the template (if any).
     * This list should typically include parameters in your search template that have default values.
     * Defaults to an empty list.
     */
    private List<String> optionalParamNames = List.of();
    /**
     * The string representation of the JsonPointer path to extract a specific field from the OpenSearch response.
     * (e.g., "/hits/hits/0/_source/my_field").
     * If null or empty, the entire OpenSearch response JSON will be used.
     * This value is compiled into {@code opensearchResponsePath}.
     * Defaults to null, meaning the entire response is taken.
     */
    private String opensearchResponsePathStr;
    /**
     * The compiled JsonPointer used to extract a specific field from the OpenSearch response JSON.
     * This is derived from {@code opensearchResponsePathStr} during validation.
     * If {@code opensearchResponsePathStr} is null or empty, this will be {@code JsonPointer.empty()},
     * indicating that the entire response should be used.
     */
    private JsonPointer opensearchResponsePath;
    /**
     * The name of the field in the Lucille Document where the extracted OpenSearch response (or a part of it)
     * will be stored.
     * Defaults to "response".
     */
    private String destinationField = "response";

    public void apply(Config config) {
      Config opensearchConfig = config.getConfig("opensearch");
      opensearchUrl = opensearchConfig.getString("url");
      opensearchIndex = opensearchConfig.getString("index");
      templateName = ConfigUtils.getOrDefault(config, "templateName", null);
      searchTemplateStr = ConfigUtils.getOrDefault(config, "searchTemplate", null);
      requiredParamNames = ConfigUtils.getOrDefault(config, "requiredParamNames", requiredParamNames);
      optionalParamNames = ConfigUtils.getOrDefault(config, "optionalParamNames", optionalParamNames);
      opensearchResponsePathStr = ConfigUtils.getOrDefault(config, "opensearchResponsePath", null);
      destinationField = ConfigUtils.getOrDefault(config, "destinationField", destinationField);
    }

    public void validate() throws StageException {
      try {
        super.validate();
      } catch (BaseConfigException e) {
        throw new StageException(e);
      }
      if ((templateName == null) == (searchTemplateStr == null)) {
        throw new StageException("You must specify templateName or searchTemplate, but not both.");
      }
      if (opensearchUrl == null || opensearchUrl.isBlank()) {
        throw new StageException("Opensearch URL cannot be null or empty.");
      }
      if (opensearchIndex == null || opensearchIndex.isBlank()) {
        throw new StageException("Opensearch index cannot be null or empty.");
      }
      if (opensearchResponsePathStr != null) {
        try {
          opensearchResponsePath = JsonPointer.compile(opensearchResponsePathStr);
        } catch (IllegalArgumentException e) {
          throw new StageException("Invalid opensearchResponsePath: " + opensearchResponsePathStr, e);
        }
      } else {
        opensearchResponsePath = JsonPointer.empty();
      }
    }

    public URI getSearchURI() {
      return URI.create(opensearchUrl + "/" + opensearchIndex + "/_search/template");
    }

    public String getTemplateName() {
      return templateName;
    }

    public String getSearchTemplateStr() {
      return searchTemplateStr;
    }

    public List<String> getRequiredParamNames() {
      return requiredParamNames;
    }

    public List<String> getOptionalParamNames() {
      return optionalParamNames;
    }

    public JsonPointer getOpensearchResponsePath() {
      return opensearchResponsePath;
    }

    public String getDestinationField() {
      return destinationField;
    }
  }

  private final QueryOpensearchConfig params;
  private final HttpClient httpClient;
  private JsonNode searchTemplateJson;

  public QueryOpensearch(Config config) throws StageException {
    super(config);

    this.params = new QueryOpensearchConfig();
    this.params.apply(config);
    this.params.validate();

    httpClient = HttpClient.newHttpClient();
  }

  @Override
  public void start() throws StageException {
    if (params.getSearchTemplateStr() == null) {
      return;
    }

    try {
      searchTemplateJson = mapper.readTree(params.getSearchTemplateStr());
    } catch (JsonProcessingException e) {
      throw new StageException("Error building JSON from the searchTemplate provided.", e);
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    ObjectNode requestJson;

    if (searchTemplateJson != null) {
      requestJson = (ObjectNode) searchTemplateJson.deepCopy();
    } else {
      requestJson = mapper.createObjectNode().put("id", params.getTemplateName());
    }

    try {
      populateJsonWithParams(doc, requestJson);
    } catch (NoSuchFieldException e) {
      log.warn("A required field was missing from Document, no query will be executed.", e);
      doc.setField("queryOpensearchError", "A requiredField was missing.");
      return null;
    }

    HttpRequest httpRequest = HttpRequest.newBuilder()
        .uri(params.getSearchURI())
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(requestJson.toString()))
        .build();

    JsonNode responseNode;

    try {
      HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
      responseNode = mapper.readTree(response.body());
    } catch (Exception e) {
      throw new StageException("Error occurred sending the Opensearch query / getting JSON.", e);
    }

    JsonNode responseFieldNode = responseNode.at(params.getOpensearchResponsePath());
    doc.setField(params.getDestinationField(), responseFieldNode);
    return null;
  }

  void populateJsonWithParams(Document doc, ObjectNode json) throws NoSuchFieldException {
    ObjectNode paramsNode = mapper.createObjectNode();

    for (String requiredParamName : params.getRequiredParamNames()) {
      if (!doc.has(requiredParamName)) {
        throw new NoSuchFieldException("Field " + requiredParamName + " is required, but missing from Document " + doc.getId() + ".");
      }

      paramsNode.set(requiredParamName, doc.getJson(requiredParamName));
    }

    for (String optionalParamName : params.getOptionalParamNames()) {
      if (doc.has(optionalParamName)) {
        paramsNode.set(optionalParamName, doc.getJson(optionalParamName));
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
