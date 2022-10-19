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

// todo  goal of the base class - not deseralization - not http client

// todo rename to AbstractRestApiStage
abstract class RestApiStageV2<T> extends Stage {

  private final static int DEFAULT_TIMEOUT = 10;
  protected final static ObjectMapper MAPPER = new ObjectMapper();

  protected final HttpClient httpClient;


  public RestApiStageV2(Config config, HttpClient client) {

    super(config);

    if (client == null) {
      throw new IllegalArgumentException("client must not be null");
    }

    this.httpClient = client;
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    return processWithResponse(doc, getResponse(buildRequest(doc)));
  }

  public abstract HttpRequest buildRequest(Document document);
  public abstract List<Document> processWithResponse(Document doc, T response) throws StageException;

  public T getResponse(HttpRequest request) throws StageException {

    try {

      // todo delegate response to the implementing class
      HttpResponse<String> response = httpClient.send(request,
        HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200) {
        throw new StageException(this.getName() + " stage failed with status code "
          + response.statusCode());
      }

      return MAPPER.readValue(response.body(), responseClass);
    } catch (IOException | InterruptedException e) {
      e.printStackTrace(); // todo consider adding trace to log
      throw new StageException(e.getMessage());
    }
  }
}
