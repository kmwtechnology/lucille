package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
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

public class SearchStaxIndexer extends Indexer {

  private static final Logger log = LoggerFactory.getLogger(SearchStaxIndexer.class);

  private final String url;
  private final String authHeader;
  private final HttpClient client;

  public SearchStaxIndexer(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix) {
    super(config, messenger, metricsPrefix);

    this.url = config.getString("indexer.url") + "/" + config.getString("indexer.defaultCollection");
    this.authHeader = getAuthorizationHeader(config);
    this.client = HttpClient.newBuilder().build();
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
    log.error("Response: " + response.body());
  }

  @Override
  public void closeConnection() {
    // no-op
  }
}