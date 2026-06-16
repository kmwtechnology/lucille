package com.kmwllc.lucille.core;


import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.kmwllc.lucille.util.LogUtils;
import com.opencsv.CSVReader;
import java.io.File;
import java.io.FileReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


public class RunnerManagerTest {

  public static final Logger log = LoggerFactory.getLogger(RunnerManagerTest.class);

  @Test
  public void testRunnerManagerFull() throws Exception {
    RunnerManager runnerManager = RunnerManager.getInstance();
    Config config = ConfigFactory.load("RunnerManagerTest/sleep.conf");
    String runId = Runner.generateRunId();

    // Ensure no lucille run is running at the start of the test
    assertFalse(runnerManager.isRunning(runId));

    String configId = runnerManager.createConfig(config);

    // Kick off a lucille run and ensure it is not skipped
    RunDetails details = runnerManager.runWithConfig(runId, configId);
    assertTrue(!details.hasThrowable());

    // Ensure the run is currently running
    assertTrue(runnerManager.isRunning(runId));

    // Expect an exception when attempting to start the same run again
    assertThrows(RunnerManagerException.class, () -> runnerManager.runWithConfig(runId, configId));

    // Wait until the run is over

    runnerManager.waitForRunCompletion(runId);

    // Ensure lucille is not running and make sure we can now kick off a new run
    assertFalse(runnerManager.isRunning(runId));
    assertTrue(!details.hasThrowable());

    // Wait for all lucille threads to finish before exiting
    runnerManager.waitForRunCompletion(runId);
  }

  @Test
  public void testWaitForRunCompletion() throws Exception {
    RunnerManager runnerManager = RunnerManager.getInstance();
    Config config = ConfigFactory.load("RunnerManagerTest/sleep.conf");
    String configId = runnerManager.createConfig(config);
    String runId = Runner.generateRunId();

    // Ensure lucille is not running first
    assertFalse(runnerManager.isRunning(runId));

    runnerManager.runWithConfig(runId, configId);
    long startTime = System.nanoTime();

    log.info("before waiting for run to complete {} ========", runnerManager.isRunning(runId));
    assertTrue(runnerManager.isRunning(runId));

    runnerManager.waitForRunCompletion(runId);

    long endTime = System.nanoTime();
    long durationMillis = (endTime - startTime) / 1_000_000;

    log.info("after waiting for run to complete {} ========", runnerManager.isRunning(runId));
    assertFalse(runnerManager.isRunning(runId));

    log.info("Test execution time: {} ms", durationMillis);
  }

  @Test
  public void testConfigLocking() throws Exception {
    RunnerManager runnerManager = RunnerManager.getInstance();
    Config config = ConfigFactory.load("RunnerManagerTest/sleep.conf");
    String configId = runnerManager.createConfig(config);

    String runId1 = Runner.generateRunId();
    String runId2 = Runner.generateRunId();

    // starts a run with this config. it is locked from concurrent runs.
    // this test has some vulnerability to a potential race condition / transient failure,
    // but the risk of this *should* be low...
    runnerManager.runWithConfig(runId1, configId, true);

    assertTrue(runnerManager.isRunning(runId1));

    // regardless of the lock param, the run should be prevented.
    assertThrows(RunnerManagerException.class, () -> runnerManager.runWithConfig(runId2, configId, false));
    assertThrows(RunnerManagerException.class, () -> runnerManager.runWithConfig(runId2, configId, true));

    runnerManager.waitForRunCompletion(runId1);
    assertFalse(runnerManager.isRunning(runId1));

    // once complete, we should be able to start the second run with the config.
    // also, note that we are able to use runId2, which we *attempted* to earlier.
    runnerManager.runWithConfig(runId2, configId, true);
    assertTrue(runnerManager.isRunning(runId2));
    runnerManager.waitForRunCompletion(runId2);
    // don't want to leave the run lingering after this test
    assertFalse(runnerManager.isRunning(runId2));
  }

  @Test
  public void testSimultaneousRuns() throws Exception {
    Config config = ConfigFactory.load("RunnerManagerTest/sleep.conf");
    RunnerManager runnerManager = RunnerManager.getInstance();
    String configId = runnerManager.createConfig(config);
    List<String> runIds = new ArrayList();
    List<CompletableFuture<Void>> futures = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      String runId = "test-run-" + i;
      CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
        try {
          runnerManager.runWithConfig(runId, configId);
        } catch (RunnerManagerException e) {
          throw new RuntimeException(e);
        }
      });
      futures.add(future);
      runIds.add(runId);
    }
    
    // blocking until all the test futures to start async runWithConfig have completed
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

    StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    while (runIds.stream().anyMatch(x -> {
      RunDetails details = runnerManager.getRunDetails(x);
      return !details.isDone();
    })) {
      if (stopWatch.getTime(TimeUnit.SECONDS) > 10) {
        fail("5 concurrent Lucille Runs are taking longer than 10 seconds to complete.");
      }
      Thread.sleep(100);
    }
    
    for (String runId : runIds) {
      RunDetails details = runnerManager.getRunDetails(runId);
      assertEquals(runId, details.getRunId());
      assertEquals(runId, details.getRunResult().getRunId());
      assertTrue(details.getRunResult().getStatus());
      assertTrue(Duration.between(details.getStartTime(), details.getEndTime()).getSeconds() >= 1,
          "Each run expected to take 1 second or more.");
    }
  }

  @Test
  public void testCreateConfigWithName() {
    RunnerManager runnerManager = RunnerManager.getInstance();
    Config config = ConfigFactory.load("RunnerManagerTest/sleep.conf");
    String configUUID = UUID.randomUUID().toString();
    boolean success = runnerManager.createConfigWithName(config, configUUID);

    assertTrue(success);

    boolean sameNameSuccess = runnerManager.createConfigWithName(config, configUUID);
    assertFalse(sameNameSuccess);
  }

  @Test
  public void testNonTrivialSimultaneousRuns() throws Exception {
    File outputFile1 = new File("output1.csv");
    File outputFile2 = new File("output2.csv");
    File outputFile3 = new File("output3.csv");

    try {
      outputFile1.delete();
      outputFile2.delete();
      outputFile3.delete();

      RunnerManager runnerManager = RunnerManager.getInstance();
      List<String> runIds = new ArrayList<>();
      List<CompletableFuture<Void>> futures = new ArrayList<>();

      for (int i = 1; i <= 3; i++) {
        Config currentConfig = ConfigFactory.load("RunnerManagerTest/imdb-" + i + ".conf");
        String configId = runnerManager.createConfig(currentConfig);
        String runId = "imdb-run-" + i;

        CompletableFuture<Void> futureImdb = CompletableFuture.runAsync(() -> {
          try {
            runnerManager.runWithConfig(runId, configId);
          } catch (RunnerManagerException e) {
            throw new RuntimeException(e);
          }
        });

        futures.add(futureImdb);
        runIds.add(runId);
      }

      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

      StopWatch stopWatch = new StopWatch();
      stopWatch.start();
      while (runIds.stream().anyMatch(x -> !runnerManager.getRunDetails(x).isDone())) {
        if (stopWatch.getTime(TimeUnit.SECONDS) > 20) {
          fail("The non-trivial, concurrent Lucille Runs are taking longer than 20 seconds to complete.");
        }
        Thread.sleep(100);
      }

      for (String runId : runIds) {
        RunDetails runDetails = runnerManager.getRunDetails(runId);
        assertTrue(runDetails.getRunResult().getStatus());
      }

      CSVReader reader1 = new CSVReader(new FileReader(outputFile1));
      CSVReader reader2 = new CSVReader(new FileReader(outputFile2));
      CSVReader reader3 = new CSVReader(new FileReader(outputFile3));

      // Run some checks on the output of each run to make sure everything was done correctly.
      validateRun1Reader(reader1);
      validateRun2Reader(reader2);
      validateRun3Reader(reader3);
    } finally {
      outputFile1.delete();
      outputFile2.delete();
      outputFile3.delete();
    }
  }

  // This is the responsibility of the Runner, but it is
  // important we ensure the RunnerManager (used by the API) does result in
  // metrics being cleaned up, preventing unbounded heap usage.
  @Test
  public void testMetricsCleanup() throws Exception {
    SharedMetricRegistries.clear();
    assertEquals(0, SharedMetricRegistries.names().size());

    // adding a dummy metric to flex that all metrics starting
    // with the run id get removed.
    SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG).counter("run-1.dummy.metric");
    SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG).counter("run-2.dummy.metric");

    RunnerManager runnerManager = RunnerManager.getInstance();
    Config noopConfig = ConfigFactory.load("RunnerManagerTest/noop.conf");
    String noopConfigId = runnerManager.createConfig(noopConfig);

    runnerManager.runWithConfig("run-1", noopConfigId);
    runnerManager.runWithConfig("run-2", noopConfigId);

    runnerManager.waitForRunCompletion("run-1");
    runnerManager.waitForRunCompletion("run-2");

    MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG);

    // Two indexer related metric names remain - these are created while validating the config.
    assertFalse(metricRegistry.getNames().stream().anyMatch(name -> name.startsWith("run-1")));
    assertFalse(metricRegistry.getNames().stream().anyMatch(name -> name.startsWith("run-2")));
    assertEquals(2, metricRegistry.getNames().size());
  }

  private void validateRun1Reader(CSVReader run1Reader) {
    for (String[] line : run1Reader) {
      assertEquals(4, line.length);

      // don't enforce that "genres" contains because there isn't enough commonality to match
      // with the pipeline config
      assertTrue(line[0].contains("REPLACEMENT"));
      assertTrue(line[2].contains("REPLACEMENT"));
      assertTrue(line[3].contains("REPLACEMENT"));
    }

    assertEquals(16, run1Reader.getLinesRead());
  }

  private void validateRun2Reader(CSVReader run2Reader) {
    for (String[] line : run2Reader) {
      assertEquals(3, line.length);

      assertFalse(line[0].contains("villain"));
      assertFalse(line[0].contains("and"));

      assertFalse(line[1].contains("villain"));
      assertFalse(line[1].contains("and"));

      assertEquals("Released", line[2]);
    }

    assertEquals(16, run2Reader.getLinesRead());
  }

  private void validateRun3Reader(CSVReader run3Reader) {
    for (String[] line : run3Reader) {
      assertEquals(5, line.length);

      // Entries 0 and 1 are the original field. Shouldn't have "REPLACEMENT".
      assertFalse(line[0].contains("REPLACEMENT"));
      assertFalse(line[1].contains("REPLACEMENT"));

      // Entries 2 and 3 are the field w/ the patterns replaced. Shouldn't have the replaced words.
      assertFalse(line[2].contains("villain"));
      assertFalse(line[2].contains("and"));

      assertFalse(line[3].contains("villain"));
      assertFalse(line[3].contains("and"));
    }

    assertEquals(16, run3Reader.getLinesRead());
  }
}
