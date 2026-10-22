package com.kmwllc.lucille.api;

import static org.junit.Assert.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Exercises the lucille-api container over HTTP: health probes, auth, config-info, rejection of an invalid
 * config, and two concurrent config upload -> run -> completed-run round trips. Each run's output is checked
 * for docs from the other run and for every pipeline stage having been applied.
 * <p>
 * Not picked up by the normal build (the name doesn't match surefire's default includes). It's run by
 * verifier/verification.sh inside the verifier container started by docker-compose.yml, after that script
 * waits for the API to become ready. There, the API is reachable at api:8080, the configs are mounted at
 * /conf, and the runs' CSV output is mounted at /output.
 */
public class VerifyIngestResult {

  private static final String API = "http://api:8080";
  // must match the password in conf/api.yml. Only the password is validated, not the username.
  private static final String AUTH_HEADER = "Basic " +
      Base64.getEncoder().encodeToString("user:4".getBytes(StandardCharsets.UTF_8));
  private static final int EXPECTED_DOCS = 500;
  private static final List<String> RUNS = List.of("a", "b");
  private static final Duration MAX_WAIT = Duration.ofMinutes(2);

  private static final HttpClient client = HttpClient.newHttpClient();
  private static final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testProbes() throws Exception {
    assertEquals(200, get("/v1/livez", false).statusCode());
    assertEquals(200, get("/v1/readyz", false).statusCode());
  }

  // auth is enabled in conf/api.yml, so protected endpoints must reject requests without credentials
  @Test
  public void testAuthEnforced() throws Exception {
    assertEquals(401, get("/v1/run", false).statusCode());
    assertEquals(200, get("/v1/run", true).statusCode());
  }

  // component scanning must work inside the shaded jar
  @Test
  public void testConnectorList() throws Exception {
    HttpResponse<String> response = get("/v1/config-info/connector-list", true);
    assertEquals(200, response.statusCode());
    JsonNode connectors = mapper.readTree(response.body());
    assertTrue("connector-list returned no connectors", connectors.isArray() && connectors.size() > 0);
  }

  @Test
  public void testInvalidConfigRejected() throws Exception {
    assertEquals(400, postConfig("{ not valid").statusCode());
  }

  /**
   * Uploads conf/run_a.conf and conf/run_b.conf, starts both runs at the same time, and checks that each run
   * succeeds and that its CSV contains exactly its own docs, with every pipeline stage applied.
   */
  @Test
  public void testConcurrentRuns() throws Exception {
    Map<String, String> configIds = new HashMap<>();
    for (String run : RUNS) {
      HttpResponse<String> response = postConfig(Files.readString(Paths.get("/conf/run_" + run + ".conf")));
      assertEquals("config upload for run " + run + " failed: " + response.body(), 200, response.statusCode());
      configIds.put(run, mapper.readTree(response.body()).get("configId").asText());
    }

    // start both runs at the same time, so they execute concurrently inside the API
    Map<String, CompletableFuture<HttpResponse<String>>> starts = new HashMap<>();
    for (String run : RUNS) {
      String body = mapper.writeValueAsString(Map.of("configId", configIds.get(run)));
      starts.put(run, client.sendAsync(request("/v1/run", true)
          .header("Content-Type", "application/json")
          .POST(HttpRequest.BodyPublishers.ofString(body))
          .build(), HttpResponse.BodyHandlers.ofString()));
    }

    Map<String, String> runIds = new HashMap<>();
    for (String run : RUNS) {
      HttpResponse<String> response = starts.get(run).join();
      assertEquals("starting run " + run + " failed: " + response.body(), 200, response.statusCode());
      runIds.put(run, mapper.readTree(response.body()).get("runId").asText());
    }
    assertNotEquals("both runs were given the same runId", runIds.get("a"), runIds.get("b"));

    Map<String, JsonNode> details = new HashMap<>();
    for (String run : RUNS) {
      JsonNode runDetails = waitForRun(runIds.get(run));
      details.put(run, runDetails);
      System.out.println("Run " + run + " details: " + runDetails);

      assertEquals(configIds.get(run), runDetails.get("configId").asText());
      assertTrue("run " + run + " did not succeed", runDetails.get("runResult").get("status").asBoolean());
      assertTrue("expected " + EXPECTED_DOCS + " docs to succeed in run " + run,
          runDetails.get("runResult").get("message").asText().contains(" " + EXPECTED_DOCS + " docs succeeded"));
    }

    // Each run writes to its own CSV (columns: id, run, num, run_label, scratch). If the runs overlapped
    // somehow, a file would contain the other run's docs, duplicates, or be missing docs. The other
    // columns check that every stage in the pipeline ran on every doc.
    for (String run : RUNS) {
      Path output = Paths.get("/output/run_" + run + ".csv");
      assertTrue("run " + run + " did not write " + output, Files.exists(output));

      List<String> actualRows = Files.readAllLines(output).stream()
          .map(line -> line.replace("\"", ""))
          .sorted()
          .collect(Collectors.toList());
      // every row should be exactly "<run>-<n>,<run>,<n>,<run>:<run>-<n>," (scratch deleted, so empty)
      List<String> expectedRows = IntStream.range(0, EXPECTED_DOCS)
          .mapToObj(n -> run + "-" + n + "," + run + "," + n + "," + run + ":" + run + "-" + n + ",")
          .sorted()
          .collect(Collectors.toList());

      List<String> missing = new ArrayList<>(expectedRows);
      missing.removeAll(actualRows);
      List<String> unexpected = new ArrayList<>(actualRows);
      unexpected.removeAll(expectedRows);
      assertTrue(output + " is missing rows (first 10): " + missing.stream().limit(10).toList(), missing.isEmpty());
      assertTrue(output + " has unexpected rows (first 10): " + unexpected.stream().limit(10).toList(), unexpected.isEmpty());
      // catches duplicated rows, which the checks above can't see
      assertEquals(output + " has the wrong number of rows", expectedRows.size(), actualRows.size());
    }

    // The runs are short, so report whether they actually overlapped in time rather than fail on it.
    double startA = details.get("a").get("startTime").asDouble();
    double endA = details.get("a").get("endTime").asDouble();
    double startB = details.get("b").get("startTime").asDouble();
    double endB = details.get("b").get("endTime").asDouble();
    System.out.println("Runs overlapped in time: " + (startA < endB && startB < endA ? "yes" : "no"));
  }

  private static JsonNode waitForRun(String runId) throws Exception {
    Instant deadline = Instant.now().plus(MAX_WAIT);
    while (true) {
      JsonNode runDetails = mapper.readTree(get("/v1/run/" + runId, true).body());
      if (!runDetails.path("endTime").isNull() && !runDetails.path("endTime").isMissingNode()) {
        return runDetails;
      }
      if (Instant.now().isAfter(deadline)) {
        fail("run " + runId + " did not complete. Last details: " + runDetails);
      }
      Thread.sleep(500);
    }
  }

  private static HttpRequest.Builder request(String path, boolean withAuth) {
    HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(API + path)).timeout(Duration.ofSeconds(30));
    if (withAuth) {
      builder.header("Authorization", AUTH_HEADER);
    }
    return builder;
  }

  private static HttpResponse<String> get(String path, boolean withAuth) throws Exception {
    return client.send(request(path, withAuth).GET().build(), HttpResponse.BodyHandlers.ofString());
  }

  private static HttpResponse<String> postConfig(String hocon) throws Exception {
    return client.send(request("/v1/config", true)
        .header("Content-Type", "application/hocon")
        .POST(HttpRequest.BodyPublishers.ofString(hocon))
        .build(), HttpResponse.BodyHandlers.ofString());
  }
}
