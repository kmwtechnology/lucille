package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class EmbedTextV3 extends RestApiStageV2 {

  private final URI requestURI;
  private final UpdateMode updateMode;
  private final List<String> fields;
  private final List<String> destinations;

  public EmbedTextV3(Config config) {
    super(config);

    this.requestURI = URI.create(config.getString("connection") + "/embed");
    this.updateMode = UpdateMode.fromConfig(config);

    fields = new ArrayList<>();
    destinations = new ArrayList<>();
    Map<String, Object> fieldMap = config.getConfig("fieldMapping").root().unwrapped();
    for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
      fields.add(entry.getKey());
      destinations.add((String) entry.getValue());
    }
  }

  /**
   *
   * @throws StageException if the field mapping is empty.
   */
  @Override
  public void start() throws StageException {
    if (fields.size() == 0 || destinations.size() == 0 || fields.size() != destinations.size())
      throw new StageException("field_mapping must have at least one source-dest pair for EmbedText");
  }

  @Override
  public HttpClient buildClient() {
    return HttpClient.newBuilder().version(DEFAULT_VERSION).connectTimeout(DEFAULT_TIMEOUT).build();
  }

  @Override
  public HttpRequest buildRequest(Document document) {

    List<String> toEmbed = new ArrayList<>();
    for (String field : fields) {
      toEmbed.add(document.getString(field));
    }

    try {
      String requestBody = MAPPER.writeValueAsString(Map.of("sentences", toEmbed));
      return HttpRequest.newBuilder()
        .uri(requestURI)
        .POST(HttpRequest.BodyPublishers.ofString(requestBody))
        .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("failed to build a POST request");
    }
  }

  @Override
  public List<Document> processWithResponse(Document doc, HttpResponse<String> response) throws StageException {

    try {
      JsonResponseV2 jsonResponse = MAPPER.readValue(response.body(), JsonResponseV2.class);
      Double[][] embeddings = jsonResponse.embeddings;


      if (embeddings.length != destinations.size()) {
        throw new StageException("Number of embeddings returned by the service does not match the " +
          "number of fields to embed.");
      }

      for (int i = 0; i < destinations.size(); i++) {
        doc.update(destinations.get(i), updateMode, embeddings[i]);
      }

    } catch (JsonProcessingException | StageException e) {
      throw new StageException("failed to parse response from EmbedText", e);
    }

    return null;
  }

  static class JsonResponseV2 {
    public String model;
    public int num_total;
    public Double[][] embeddings;
  }
}
