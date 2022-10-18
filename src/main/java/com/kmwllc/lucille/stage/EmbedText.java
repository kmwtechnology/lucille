package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * This stage will embed the specified field in the document. Field mapping specifies the name of
 * the field to embed and the name the embedded field will be stored under. Connection url specifies
 * the base path to the embedding service.
 * <p>
 * todo check if update mode is needed here
 */
public class EmbedText extends Stage {

  private final static ObjectMapper MAPPER = new ObjectMapper();

  private final String cncUrl;
  private final UpdateMode updateMode;
  private final HttpClient httpClient;
  private final Map<String, Object> fieldMap;


  public EmbedText (Config config) {
    super(config); // todo add optional / required once merged with the stage-validation branch

    this.cncUrl = config.getString("connection");
    this.updateMode = UpdateMode.fromConfig(config);
    this.fieldMap = config.getConfig("fieldMapping").root().unwrapped();

    this.httpClient = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(10))
      .build();
  }

  /**
   *
   * @throws StageException if the field mapping is empty.
   */
  @Override
  public void start() throws StageException {
    if (fieldMap.size() == 0)
      throw new StageException("field_mapping must have " +
        "at least one source-dest pair for EmbedText");
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {

    // todo if there are multiple fields - do a multi sent request
    // todo use the post

    // For each field, if this document has the source field, rename it to the destination field
    for (Map.Entry<String, Object> fieldPair : fieldMap.entrySet()) {

      if (!doc.has(fieldPair.getKey())) {
        continue;
      }

      String dest = (String) fieldPair.getValue();
      String toEmbed = doc.getString(fieldPair.getKey());
      doc.update(dest, updateMode, getEmbedding(toEmbed));
    }
    return null;
  }

  private Double[] getEmbedding(String toEmbed) throws StageException {

    HttpRequest request = buildGetRequest(toEmbed);

    try {
      HttpResponse<String> response = httpClient.send(request,
        HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200) {
        throw new StageException("EmbedText failed with status code " + response.statusCode());
      }

      JsonResponse jsonResponse = MAPPER.readValue(response.body(), JsonResponse.class);
      return jsonResponse.embedding;

    } catch (IOException | InterruptedException e) {
      e.printStackTrace(); // todo consider adding trace to log
      throw new StageException(e.getMessage());
    }
  }

  private HttpRequest buildGetRequest(String toEmbed) {
    String sentenceEncoded = encodeValue(toEmbed);
    String url = this.cncUrl + "/embed?sentence=" + sentenceEncoded;
    return HttpRequest.newBuilder()
      .GET()
      .uri(URI.create(url))
      .setHeader("User-Agent", "Java 11 HttpClient Bot") // add request header
      .build();
  }

  // Method to encode a string value using `UTF-8` encoding scheme
  private static String encodeValue(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  static class JsonResponse {
    public String model;
    public int num_total;
    public Double[] embedding;
  }
}
