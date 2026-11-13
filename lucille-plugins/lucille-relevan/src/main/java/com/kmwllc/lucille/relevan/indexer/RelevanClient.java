package com.kmwllc.lucille.relevan.indexer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * A minimal HTTP client for the Relevan REST API. It sends requests and parses JSON responses; deciding what a status
 * code means is left to the caller.
 */
class RelevanClient implements AutoCloseable {

  /** A response from the Relevan API. {@code body} is null when the response is empty or not JSON. */
  record Response(int status, JsonNode body, String rawBody) {

    boolean isSuccess() {
      return status >= 200 && status < 300;
    }
  }

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final HttpClient httpClient;
  private final String baseUrl;
  private final String apiKey;
  private final Duration requestTimeout;

  RelevanClient(String baseUrl, String apiKey, Duration requestTimeout) {
    this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    this.apiKey = apiKey;
    this.requestTimeout = requestTimeout;
    this.httpClient = HttpClient.newBuilder().connectTimeout(requestTimeout).build();
  }

  /** Encodes a single path segment, such as an org ID, index name or document ID. */
  static String segment(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
  }

  Response get(String path) throws IOException {
    return send(request(path).GET().build());
  }

  Response post(String path, Object body) throws IOException {
    byte[] json = MAPPER.writeValueAsBytes(body);
    return send(request(path)
        .header("content-type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofByteArray(json))
        .build());
  }

  CompletableFuture<Response> deleteAsync(String path) {
    return httpClient.sendAsync(request(path).DELETE().build(), HttpResponse.BodyHandlers.ofString())
        .thenApply(RelevanClient::toResponse);
  }

  private HttpRequest.Builder request(String path) {
    return HttpRequest.newBuilder(URI.create(baseUrl + path))
        .timeout(requestTimeout)
        .header("x-api-key", apiKey)
        .header("accept", "application/json");
  }

  private Response send(HttpRequest request) throws IOException {
    try {
      return toResponse(httpClient.send(request, HttpResponse.BodyHandlers.ofString()));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InterruptedIOException("Interrupted while waiting for Relevan: " + request.uri());
    }
  }

  private static Response toResponse(HttpResponse<String> response) {
    String raw = response.body();
    JsonNode body = null;
    if (raw != null && !raw.isBlank()) {
      try {
        body = MAPPER.readTree(raw);
      } catch (IOException e) {
        // not JSON, e.g. an HTML error page from a proxy; callers fall back to rawBody
      }
    }
    return new Response(response.statusCode(), body, raw);
  }

  @Override
  public void close() {
    httpClient.close();
  }
}
