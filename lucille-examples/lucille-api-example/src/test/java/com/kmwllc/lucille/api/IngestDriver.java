package com.kmwllc.lucille.api;

import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Drives the ingest through the API: uploads conf/run_a.conf as HOCON and conf/run_b.json as JSON, starts both
 * runs at the same time, and waits for both to succeed. Throws (so the JVM exits non-zero) if an upload or run fails.
 * <p>
 * Run first by the verifier container in docker-compose.yml, where the configs are mounted at /conf. If this
 * completes successfully, the verifier then checks the runs' output in VerifyIngestResult.
 */
public class IngestDriver {

  static final int EXPECTED_DOCS = 500;
  static final List<String> RUNS = List.of("a", "b");
  private static final Duration MAX_WAIT = Duration.ofMinutes(2);
  // one config per content type, so both of the ways /v1/config accepts a config are exercised
  private static final Map<String, String> CONFIG_FILES = Map.of("a", "run_a.conf", "b", "run_b.json");

  public static void main(String[] args) throws Exception {
    Map<String, String> configIds = new HashMap<>();
    for (String run : RUNS) {
      HttpResponse<String> response = uploadConfig(run);
      check(response.statusCode() == 200, "config upload for run " + run + " failed: " + response.body());
      configIds.put(run, ApiClient.mapper.readTree(response.body()).get("configId").asText());
    }

    // start both runs at the same time, so they execute concurrently inside the API
    Map<String, CompletableFuture<HttpResponse<String>>> starts = new HashMap<>();
    for (String run : RUNS) {
      starts.put(run, ApiClient.startRun(configIds.get(run)));
    }

    Map<String, String> runIds = new HashMap<>();
    for (String run : RUNS) {
      HttpResponse<String> response = starts.get(run).join();
      check(response.statusCode() == 200, "starting run " + run + " failed: " + response.body());
      runIds.put(run, ApiClient.mapper.readTree(response.body()).get("runId").asText());
    }
    check(!runIds.get("a").equals(runIds.get("b")), "both runs were given the same runId");

    Map<String, JsonNode> details = new HashMap<>();
    for (String run : RUNS) {
      JsonNode runDetails = waitForRun(runIds.get(run));
      details.put(run, runDetails);
      System.out.println("Run " + run + " details: " + runDetails);

      check(configIds.get(run).equals(runDetails.get("configId").asText()),
          "run " + run + " has the wrong configId: " + runDetails.get("configId"));
      check(runDetails.get("runResult").get("status").asBoolean(), "run " + run + " did not succeed");
      check(runDetails.get("runResult").get("message").asText().contains(" " + EXPECTED_DOCS + " docs succeeded"),
          "expected " + EXPECTED_DOCS + " docs to succeed in run " + run);
    }

    // The runs are short, so report whether they actually overlapped in time rather than fail on it.
    double startA = details.get("a").get("startTime").asDouble();
    double endA = details.get("a").get("endTime").asDouble();
    double startB = details.get("b").get("startTime").asDouble();
    double endB = details.get("b").get("endTime").asDouble();
    System.out.println("Runs overlapped in time: " + (startA < endB && startB < endA ? "yes" : "no"));
  }

  private static HttpResponse<String> uploadConfig(String run) throws Exception {
    String file = CONFIG_FILES.get(run);
    String contentType = file.endsWith(".json") ? "application/json" : "application/hocon";
    System.out.println("Uploading " + file + " as " + contentType);
    return ApiClient.postConfig(Files.readString(Paths.get("/conf", file)), contentType);
  }

  private static JsonNode waitForRun(String runId) throws Exception {
    Instant deadline = Instant.now().plus(MAX_WAIT);
    while (true) {
      JsonNode runDetails = ApiClient.mapper.readTree(ApiClient.get("/v1/run/" + runId, true).body());
      if (!runDetails.path("endTime").isNull() && !runDetails.path("endTime").isMissingNode()) {
        return runDetails;
      }
      check(Instant.now().isBefore(deadline), "run " + runId + " did not complete. Last details: " + runDetails);
      Thread.sleep(500);
    }
  }

  private static void check(boolean condition, String message) {
    if (!condition) {
      throw new IllegalStateException(message);
    }
  }
}
