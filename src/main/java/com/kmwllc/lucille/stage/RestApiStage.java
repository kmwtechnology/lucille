package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

abstract class RestApiStage extends Stage {

  // constant variables
  protected final static ObjectMapper MAPPER = new ObjectMapper();
  protected final static Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10);
  protected final static HttpClient.Version DEFAULT_VERSION = HttpClient.Version.HTTP_1_1;

  protected final HttpClient client;

  public RestApiStage(Config config, StageSpec spec) {
    super(config, spec);
    this.client = getNonNull(buildClient());
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    HttpRequest request = getNonNull(buildRequest(doc));
    try {
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
      return processWithResponse(doc, response);
    } catch (IOException | InterruptedException e) {
      throw new StageException("Failed to retrieve response from API", e);
    }
  }

  public HttpClient buildClient() {
    return HttpClient.newBuilder().version(DEFAULT_VERSION).connectTimeout(DEFAULT_TIMEOUT).build();
  }

  public abstract HttpRequest buildRequest(Document document);

  public abstract List<Document> processWithResponse(Document doc, HttpResponse<String> response) throws StageException;

  private static <T> T getNonNull(T item) {
    if (item == null) {
      throw new IllegalArgumentException("expecting non null parameter");
    }
    return item;
  }
}
