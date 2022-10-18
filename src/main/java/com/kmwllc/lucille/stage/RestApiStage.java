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

abstract class RestApiStage<T> extends Stage {

  private final static int DEFAULT_TIMEOUT = 10;
  protected final static ObjectMapper MAPPER = new ObjectMapper();

  protected final HttpClient httpClient;
  private final Class<T> responseClass;


  public RestApiStage(Config config, Class<T> responseClass) {
    this(config, responseClass, DEFAULT_TIMEOUT);
  }

  public RestApiStage(Config config, Class<T> responseClass, int timeout) {
    super(config);

    if (timeout < 1) {
      throw new IllegalArgumentException("timeout must be >= 1");
    }

    this.responseClass = responseClass;
    this.httpClient = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(timeout))
      .build();
  }

  public T getResponse(HttpRequest request) throws StageException {

    try {

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

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    return processWithResponse(doc, getResponse(buildRequest(doc)));
  }

  public abstract HttpRequest buildRequest(Document document);
  public abstract List<Document> processWithResponse(Document doc, T response) throws StageException;
}
