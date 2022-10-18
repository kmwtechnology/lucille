package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;

import java.net.URI;
import java.net.http.HttpRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// todo is there a way to only specify JsonResponseV2 once
public class EmbedTextV2 extends RestApiStage<EmbedTextV2.JsonResponseV2> {

  private final String cncUrl;
  private final UpdateMode updateMode;
  private final List<String> fields;
  private final List<String> destinations;

  public EmbedTextV2(Config config) {
    super(config, JsonResponseV2.class);

    this.cncUrl = config.getString("connection");
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
  public HttpRequest buildRequest(Document document) {

    List<String> toEmbed = new ArrayList<>();
    for (String field : fields) {
      toEmbed.add(document.getString(field));
    }

    try {
      String requestBody = MAPPER.writeValueAsString(Map.of("sentences", toEmbed));
      return HttpRequest.newBuilder()
        .uri(URI.create(cncUrl + "/embed"))
        .POST(HttpRequest.BodyPublishers.ofString(requestBody))
        .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("failed to build a POST request");
    }
  }

  @Override
  public List<Document> processWithResponse(Document doc, JsonResponseV2 response)
    throws StageException {

    Double[][] embeddings = response.embeddings;
    if (embeddings.length != destinations.size()) {
      throw new StageException("Number of embeddings returned by the service does not match the " +
        "number of fields to embed.");
    }
    for (int i = 0; i < destinations.size(); i++) {
      doc.update(destinations.get(i), updateMode, embeddings[i]);
    }

    return null;
  }

  static class JsonResponseV2 {
    public String model;
    public int num_total;
    public Double[][] embeddings;
  }
}
