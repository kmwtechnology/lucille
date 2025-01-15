package com.kmwllc.lucille.core;


import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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

  public void setUp() {
    // clear config lib
    RunnerManager.getInstance().clearConfig();
    // clear history
    RunnerManager.getInstance().clearAllRunDetails();
  }

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
    assertTrue(details.getErrorCount() == 0);

    // Ensure the run is currently running
    assertTrue(runnerManager.isRunning(runId));

    // Expect an exception when attempting to start the same run again
    assertThrows(RunnerManagerException.class, () -> runnerManager.runWithConfig(runId, configId));

    // Wait until the run is over
    runnerManager.waitForRunCompletion(runId);

    // Ensure lucille is not running and make sure we can now kick off a new run
    assertFalse(runnerManager.isRunning(runId));
    assertTrue(details.getErrorCount() == 0);

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
  public void testSimultaneousRuns() throws Exception {
    Config config = ConfigFactory.load("RunnerManagerTest/sleep.conf");
    RunnerManager runnerManager = RunnerManager.getInstance();
    String configId = runnerManager.createConfig(config);
    List<String> runIds = new ArrayList();

    for (int i = 0; i < 5; i++) {
      String runId = "test-run-" + i;
      // runnerManager.runWithConfig is non-blocking so we don't need to invoke it here via
      // CompleteableFuture.runAsync()
      // but this approach simulates a scenerio where multiple threads are calling it concurrently
      CompletableFuture.runAsync(() -> {
        try {
          runnerManager.runWithConfig(runId, configId);
          assertFalse(runnerManager.getRunDetails(runId).isDone());
        } catch (RunnerManagerException e) {
          throw new RuntimeException(e);
        }
      });
      runIds.add(runId);
    }

    assertEquals(5, runIds.size());

    StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    while (runIds.stream().anyMatch(x -> {
      RunDetails details = runnerManager.getRunDetails(x);
      if (details == null) {
        log.info("RunDetails is null for runId: " + x);
      }
      return details == null || !details.isDone();
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

}
