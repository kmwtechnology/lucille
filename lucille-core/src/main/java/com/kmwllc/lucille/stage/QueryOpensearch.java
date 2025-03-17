package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

/**
 * A stage for running a search request on an Opensearch index, specifying a field in the response, and putting that field's value
 * on a Document. The specified query can either be found in a Document's field or specified in your Config.
 *
 * opensearch (Map): Configuration for your Opensearch instance. Should contain the url to your Opensearch instance, the index
 * name you want to query on, and whether invalid certificates can be accepted. See the opensearch ingest example for an
 * example of the configuration needed. (Be sure to include it as part of the Stage's Config.)
 *
 * opensearchQuery (String, Optional): The query to run against your OpenSearch index. If not specified, queries will only be run
 * for documents that have the documentQueryField.
 *
 * opensearchResponsePath (String, Optional): A path to a field in the Opensearch response whose value you want to place on a Lucille Document.
 * Use JsonPointer notation. An IllegalArgumentException will be thrown if the path has invalid formatting. Defaults to using the entire
 * response.
 *
 * documentQueryField (String, Optional): The field in a Lucille document that contains the query you want to run against OpenSearch index. If not present
 * on a document, the opensearchQuery will be run instead, if it is specified; otherwise, no query will be run for the document. If not specified, the
 * opensearchQuery will be applied for every document.
 *
 * destinationField (String, Optional): The name of the field you'll write the response value to in a Lucille Document. Defaults to "response".
 *
 */
public class QueryOpensearch extends Stage {

  private final URI opensearchURI;

  private final String opensearchQuery;
  private final JsonPointer opensearchResponsePath;

  private final String documentQueryField;
  private final String destinationField;

  private final HttpClient httpClient;

  private String opensearchQueryResponse;

  public QueryOpensearch(Config config) {
    super(config, new StageSpec()
        .withRequiredParents("opensearch")
        .withOptionalProperties("opensearchQuery", "opensearchResponsePath", "documentQueryField", "destinationField"));

    this.opensearchURI = getOpensearchURI(config);

    this.opensearchQuery = ConfigUtils.getOrDefault(config, "opensearchQuery", null);

    if (config.hasPath("opensearchResponsePath")) {
      this.opensearchResponsePath = JsonPointer.compile(config.getString("opensearchResponsePath"));
    } else {
      this.opensearchResponsePath = JsonPointer.empty();
    }

    this.documentQueryField = ConfigUtils.getOrDefault(config, "documentQueryField", null);
    this.destinationField = ConfigUtils.getOrDefault(config, "destinationField", "response");

    if (opensearchQuery == null && documentQueryField == null) {
      throw new IllegalArgumentException("opensearchQuery and documentQueryField cannot both be null.");
    }

    httpClient = HttpClient.newHttpClient();
  }

  @Override
  public void start() throws StageException {
    // Run the opensearchQuery, if it exists, and store the result, so we only have to run it once.
    if (opensearchQuery == null) {
      return;
    }

    HttpRequest request = HttpRequest.newBuilder()
        .uri(opensearchURI)
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(opensearchQuery))
        .build();

    try {
      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode currentNode = objectMapper.readTree(response.body());

      JsonNode responseFieldNode = currentNode.at(opensearchResponsePath);
      opensearchQueryResponse = responseFieldNode.toString();
    } catch (Exception e) {
      throw new StageException("Error occurred executing the Opensearch Query.", e);
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    // falling back on the opensearchQuery, if it exists.
    if (documentQueryField == null || !doc.has(documentQueryField)) {
      if (opensearchQueryResponse != null) {
        doc.setField(destinationField, opensearchQueryResponse);
      }

      return null;
    }

    String query = doc.getString(documentQueryField);

    HttpRequest request = HttpRequest.newBuilder()
        .uri(opensearchURI)
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(query))
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

  /**
   * Returns a URI to opensearch based on the config provided by the user (namely, the opensearch Map they provide.)
   */
  private URI getOpensearchURI(Config config) {
    String uriString = OpenSearchUtils.getOpenSearchUrl(config) + "/" + OpenSearchUtils.getOpenSearchIndex(config) + "/_search";
    return URI.create(uriString);
  }
}
