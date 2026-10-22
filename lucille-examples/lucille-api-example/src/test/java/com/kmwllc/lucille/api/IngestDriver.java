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
    Map<String, String> configIds = uploadConfigs();
    Map<String, String> runIds = startRuns(configIds);
    Map<String, JsonNode> details = awaitRuns(runIds, configIds);
    reportOverlap(details);
  }

  // returns each run's configId
  private static Map<String, String> uploadConfigs() throws Exception {
    Map<String, String> configIds = new HashMap<>();
    for (String run : RUNS) {
      String file = CONFIG_FILES.get(run);
      String contentType = file.endsWith(".json") ? "application/json" : "application/hocon";
      System.out.println("Uploading " + file + " as " + contentType);
      HttpResponse<String> response = ApiClient.postConfig(Files.readString(Paths.get("/conf", file)), contentType);
      check(response.statusCode() == 200, "config upload for run " + run + " failed: " + response.body());
      configIds.put(run, ApiClient.mapper.readTree(response.body()).get("configId").asText());
    }
    return configIds;
  }

  // Starts every run before waiting on any of them, so they execute concurrently inside the API. Returns each run's runId.
  private static Map<String, String> startRuns(Map<String, String> configIds) throws Exception {
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
    return runIds;
  }

  // waits for every run to finish and checks that each succeeded. Returns each run's final details.
  private static Map<String, JsonNode> awaitRuns(Map<String, String> runIds, Map<String, String> configIds)
      throws Exception {
    Map<String, JsonNode> details = new HashMap<>();
    for (String run : RUNS) {
      JsonNode runDetails = waitForRun(runIds.get(run));
      System.out.println("Run " + run + " details: " + runDetails);
      checkRunSucceeded(run, configIds.get(run), runDetails);
      details.put(run, runDetails);
    }
    return details;
  }

  private static void checkRunSucceeded(String run, String configId, JsonNode runDetails) {
    check(configId.equals(runDetails.get("configId").asText()),
        "run " + run + " has the wrong configId: " + runDetails.get("configId"));
    // runResult is null if the run threw, rather than completing with a failed status
    JsonNode runResult = runDetails.path("runResult");
    check(runResult.path("status").asBoolean(), "run " + run + " did not succeed: " + runDetails);
    check(runResult.path("message").asText().contains(" " + EXPECTED_DOCS + " docs succeeded"),
        "expected " + EXPECTED_DOCS + " docs to succeed in run " + run + ": " + runDetails);
  }

  // The runs are short, so report whether they actually overlapped in time rather than fail on it.
  private static void reportOverlap(Map<String, JsonNode> details) {
    double startA = details.get("a").get("startTime").asDouble();
    double endA = details.get("a").get("endTime").asDouble();
    double startB = details.get("b").get("startTime").asDouble();
    double endB = details.get("b").get("endTime").asDouble();
    System.out.println("Runs overlapped in time: " + (startA < endB && startB < endA ? "yes" : "no"));
  }

  private static JsonNode waitForRun(String runId) throws Exception {
    Instant deadline = Instant.now().plus(MAX_WAIT);
    while (true) {
      HttpResponse<String> response = ApiClient.get("/v1/run/" + runId, true);
      check(response.statusCode() == 200, "getting run " + runId + " failed: " + response.body());
      JsonNode runDetails = ApiClient.mapper.readTree(response.body());
      // "done" (rather than endTime) is only true once the run has finished, so runResult is set by then
      // if the run didn't throw. endTime is set just before runResult, so a poll could see one without the other.
      if (runDetails.path("done").asBoolean()) {
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
