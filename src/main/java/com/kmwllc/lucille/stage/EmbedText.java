package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.stream.Collectors;


/**
 * This stage will retrieve the embedding for the source fields in the document and store them in the target fields.
 */
public class EmbedText extends RestApiStage {

  private final URI requestURI;
  private final UpdateMode updateMode;
  private final Map<String, String> mapping;

  // TODO consider testing the connection in the start

  public EmbedText(Config config) {
    super(config, new StageSpec().withRequiredParents("fieldMapping").withRequiredProperties("uri"));

    this.requestURI = URI.create(config.getString("uri"));
    this.updateMode = UpdateMode.fromConfig(config);
    this.mapping = new LinkedHashMap<>();

    // temporary variables
    Map<String, Object> fieldMap = config.getConfig("fieldMapping").root().unwrapped();
    Set<String> seenTargets = new HashSet<>();

    for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
      String source = entry.getKey();
      String target = (String) entry.getValue();

      if (mapping.containsKey(source)) {
        throw new IllegalArgumentException("Duplicate source field: " + source);
      }

      if (seenTargets.contains(target)) {
        throw new IllegalArgumentException("Duplicate target field: " + target);
      } else {
        seenTargets.add(target);
      }

      mapping.put(source, target);
    }

    if (mapping.isEmpty()) {
      throw new IllegalArgumentException("No field mapping provided");
    }
  }

  @Override
  public HttpRequest buildRequest(Document document) {

    List<String> toEmbed = mapping.keySet().stream().map(document::getString)
      .collect(Collectors.toList());

    try {
      String requestBody = MAPPER.writeValueAsString(Map.of("sentences", toEmbed));
      // todo see if there are any headers from the client that we want to include (like auth)
      return HttpRequest.newBuilder()
        .uri(requestURI)
        .POST(HttpRequest.BodyPublishers.ofString(requestBody))
        .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("failed to build a POST request");
    }
  }

  static class JsonResponse {
    public String model;
    public int num_total;
    public Double[][] embeddings;
  }

  @Override
  public List<Document> processWithResponse(Document doc, HttpResponse<String> response) throws StageException {

    try {
      JsonResponse jsonResponse = MAPPER.readValue(response.body(), JsonResponse.class);
      Double[][] embeddings = jsonResponse.embeddings;


      if (embeddings.length != mapping.size()) {
        throw new StageException("Number of embeddings returned by the service does not match the " +
          "number of fields to embed.");
      }

      // todo this assumes that embeddings are returned in the order as the fields in the request
      int index = 0;
      for (Map.Entry<String, String> entry : mapping.entrySet()) {
        String target = entry.getValue();

        doc.update(target, updateMode, embeddings[index]);
        index++;

      }

    } catch (JsonProcessingException | StageException e) {
      throw new StageException("failed to parse response from EmbedText", e);
    }

    return null;
  }
}
