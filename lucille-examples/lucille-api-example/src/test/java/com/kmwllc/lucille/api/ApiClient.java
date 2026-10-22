package com.kmwllc.lucille.api;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * HTTP helpers for talking to the api container from the verifier container in docker-compose.yml,
 * where the API is reachable at api:8080.
 */
public final class ApiClient {

  private static final String API = "http://api:8080";
  // must match the password in conf/api.yml. Only the password is validated, not the username.
  private static final String AUTH_HEADER = "Basic " +
      Base64.getEncoder().encodeToString("user:4".getBytes(StandardCharsets.UTF_8));

  private static final HttpClient client = HttpClient.newHttpClient();
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private ApiClient() {
  }

  static HttpResponse<String> get(String path, boolean withAuth) throws Exception {
    return client.send(request(path, withAuth).GET().build(), HttpResponse.BodyHandlers.ofString());
  }

  // contentType is "application/hocon" or "application/json", the two that /v1/config accepts
  static HttpResponse<String> postConfig(String body, String contentType) throws Exception {
    return client.send(request("/v1/config", true)
        .header("Content-Type", contentType)
        .POST(HttpRequest.BodyPublishers.ofString(body))
        .build(), HttpResponse.BodyHandlers.ofString());
  }

  // async, so that several runs can be started at the same time
  static CompletableFuture<HttpResponse<String>> startRun(String configId) throws Exception {
    String body = MAPPER.writeValueAsString(Map.of("configId", configId));
    return client.sendAsync(request("/v1/run", true)
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body))
        .build(), HttpResponse.BodyHandlers.ofString());
  }

  private static HttpRequest.Builder request(String path, boolean withAuth) {
    HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(API + path)).timeout(Duration.ofSeconds(30));
    if (withAuth) {
      builder.header("Authorization", AUTH_HEADER);
    }
    return builder;
  }
}
