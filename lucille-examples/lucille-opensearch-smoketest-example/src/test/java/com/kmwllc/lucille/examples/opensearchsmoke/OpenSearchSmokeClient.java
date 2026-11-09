package com.kmwllc.lucille.examples.opensearchsmoke;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;

/**
 * Plain HTTP calls to the index the smoke test writes, used by the verifiers. Uses OPENSEARCH_URL and OPENSEARCH_INDEX
 * when set, and the same defaults as conf/opensearch-smoke.conf otherwise.
 */
final class OpenSearchSmokeClient {

  static final String MATCH_ALL = "{\"query\": {\"match_all\": {}}}";

  private static final String BASE_URL = System.getenv().getOrDefault("OPENSEARCH_URL", "http://localhost:9200");
  private static final String INDEX = System.getenv().getOrDefault("OPENSEARCH_INDEX", "smoke-test");
  private static final HttpClient HTTP = HttpClient.newHttpClient();
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private OpenSearchSmokeClient() {
  }

  // Indexing is near real time, so each verifier refreshes before it counts.
  static void refresh() throws IOException, InterruptedException {
    HttpResponse<String> response = send("POST", "/" + INDEX + "/_refresh", null);
    if (response.statusCode() != 200) {
      throw new IOException("refresh failed with " + response.statusCode() + ": " + response.body());
    }
  }

  // Counts the documents matching the given query body, e.g. MATCH_ALL or a term query.
  static long count(String queryJson) throws IOException, InterruptedException {
    HttpResponse<String> response = send("POST", "/" + INDEX + "/_count", queryJson);
    if (response.statusCode() != 200) {
      throw new IOException("count failed with " + response.statusCode() + ": " + response.body());
    }
    JsonNode json = MAPPER.readTree(response.body());
    return json.get("count").asLong();
  }

  // The term query that matches documents by one keyword field.
  static String termQuery(String field, String value) throws IOException {
    return MAPPER.writeValueAsString(MAPPER.createObjectNode()
        .set("query", MAPPER.createObjectNode().set("term", MAPPER.createObjectNode().put(field, value))));
  }

  // The HTTP status of fetching one document by id: 200 if it exists, 404 if it does not.
  static int documentStatus(String id) throws IOException, InterruptedException {
    return send("GET", "/" + INDEX + "/_doc/" + id, null).statusCode();
  }

  private static HttpResponse<String> send(String method, String path, String body) throws IOException, InterruptedException {
    HttpRequest.BodyPublisher publisher = body == null ? BodyPublishers.noBody() : BodyPublishers.ofString(body);
    HttpRequest request = HttpRequest.newBuilder(URI.create(BASE_URL + path))
        .method(method, publisher)
        .header("Content-Type", "application/json")
        .build();
    return HTTP.send(request, BodyHandlers.ofString());
  }
}
