package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.typesafe.config.Config;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Base64;
import java.util.List;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Indexer for SearchStax Indexes. Uses the HttpClient to send documents up to the SearchStax /update API.
 * Config Parameters:
 *
 *   - url (String) : The URL where SearchStax is being hosted. This should not include the collection name or "update"/"emselect"
 *   - defaultCollection (String) : The name of the collection to index documents into
 *   - userName (String): Required if `token` is not present. username to use for basic authentication.
 *   - password (String): Required if `token` is not present. password to use for basic authentication.
 *   - token (String): Required if `userNmae` and `password` are not present. Bearer Token to be used for authentication.
 */
public class SearchStaxIndexer extends Indexer {

  private static final Logger log = LoggerFactory.getLogger(SearchStaxIndexer.class);

  private final String url;
  private final String authHeader;
  private final HttpClient client;
  private final boolean bypass;

  public SearchStaxIndexer(Config config, IndexerMessenger messenger, String metricsPrefix) {
    this(config, messenger, false, metricsPrefix);
  }

  // TODO: What is the bypass param for?
  public SearchStaxIndexer(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix) {
    this(config, messenger, bypass, metricsPrefix, null);
  }

  public SearchStaxIndexer(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix, HttpClient client) {
    super(config, messenger, metricsPrefix);

    this.url = config.getString("indexer.url") + "/" + config.getString("indexer.defaultCollection");
    this.authHeader = getAuthorizationHeader(config);
    this.client = client != null ? client : HttpClient.newBuilder().build();
    this.bypass = bypass;
  }

  private static String getAuthorizationHeader(Config config) {
    boolean hasUserName = config.hasPath("indexer.userName");
    boolean hasPassword = config.hasPath("indexer.password");
    boolean hasToken = config.hasPath("indexer.token");

    if (hasPassword || hasUserName) {
      if (hasPassword != hasUserName) {
        log.error("Both the userName and password are required for Basic Authentication");
      }

      String credentials = config.getString("indexer.userName") + ":" + config.getString("indexer.password");
      return "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes());
    } else if (hasToken) {
      return "Bearer " + config.getString("indexer.token");
    } else {
      log.error("Authorization is required for the SearchStaxIndexer.");
      return "";
    }
  }

  @Override
  public boolean validateConnection() {
    HttpRequest request = HttpRequest.newBuilder()
        .GET()
        .uri(URI.create(this.url + "/emselect"))
        .header("Authorization", this.authHeader)
        .build();

    try {
      HttpResponse<String> response = this.client.send(request, BodyHandlers.ofString());
      int status = response.statusCode();

      return status == HttpStatus.OK_200;
    } catch (Exception e) {
      log.error("Failed to validate the SearchStax connection", e);
      return false;
    }
  }

  @Override
  protected void sendToIndex(List<Document> documents) throws Exception {
    StringBuilder documentJSON = new StringBuilder().append("[");
    for (Document doc : documents) {
      documentJSON.append("\n").append(doc.toString()).append(",");
    }
    documentJSON.append("\n]");

    BodyPublisher publisher = HttpRequest.BodyPublishers.ofString(documentJSON.toString());

    HttpRequest request = HttpRequest.newBuilder()
        .POST(publisher)
        .uri(URI.create(this.url + "/update"))
        .header("Authorization", this.authHeader)
        .header("Content-Type", "application/json")
        .build();

    HttpResponse<String> response = this.client.send(request, BodyHandlers.ofString());

    if (response.statusCode() != HttpStatus.OK_200) {
      throw new IndexerException("Failed to upload documents to SearchStax index. Response: " + response.body());
    }
  }

  @Override
  public void closeConnection() {
    // no-op
  }
}