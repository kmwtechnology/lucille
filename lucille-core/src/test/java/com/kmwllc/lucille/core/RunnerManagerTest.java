package com.kmwllc.lucille.core;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


public class RunnerManagerTest {
  
  public static final Logger log = LoggerFactory.getLogger(RunnerManagerTest.class);

  final String runId = "runId";

  @Test
  public void testRunnerManagerFull() throws Exception {
    RunnerManager runnerManager = RunnerManager.getInstance();
    Config config = ConfigFactory.load("RunnerManagerTest/sleep.conf");

    // Ensure no lucille run is running at the start of the test
    assertFalse(runnerManager.isRunning(runId));

    String configId = runnerManager.createConfig(config);

    // Kick off a lucille run and ensure it is not skipped
    assertTrue(runnerManager.runWithConfig(runId, configId));

    // While we lucille is running, ensure lucille isRunning and a new run is skipped
    assertTrue(runnerManager.isRunning(runId));
    assertFalse(runnerManager.runWithConfig(runId, configId));

    // Wait until the run is over
    runnerManager.waitForRunCompletion(runId);

    // Ensure lucille is not running and make sure we can now kick off a new run
    assertFalse(runnerManager.isRunning(runId));
    assertTrue(runnerManager.runWithConfig(runId, configId));

    // Wait for all lucille threads to finish before exiting
    runnerManager.waitForRunCompletion(runId);
  }

  @Test
  public void testWaitForRunCompletion() throws Exception {
    RunnerManager runnerManager = RunnerManager.getInstance();
    Config config = ConfigFactory.load("RunnerManagerTest/sleep.conf");
    String configId = runnerManager.createConfig(config);

    // Ensure lucille is not running first
    assertFalse(runnerManager.isRunning(runId));

    runnerManager.runWithConfig(runId, configId);

    // Ensure lucille is running, wait for it stop and ensure its stopped
    assertTrue(runnerManager.isRunning(runId));
    runnerManager.waitForRunCompletion(runId);
    assertFalse(runnerManager.isRunning(runId));
  }


  @Test
  public void testSimultaneousRuns() throws Exception {
    int numRuns = 5;
    log.info("Starting testSimultaneousRuns with {} runs", numRuns);
    CountDownLatch latch = new CountDownLatch(numRuns);
    RunnerManager runnerManager = RunnerManager.getInstance();
    Config c = ConfigFactory.load("conf/simple-config.conf");
    String configId = runnerManager.createConfig(c);

    for (int i = 0; i < numRuns; i++) {
      final String runId = "test-run-" + i;
      

      new Thread(() -> {
        try {
          boolean started = runnerManager.runWithConfig(runId, configId);
          log.info("Simultaneous Run {} started: {}", runId, started);
        } catch (Exception e) {
          log.error("Error in run {}", runId, e);
        } finally {
          latch.countDown();
        }
      }).start();
    }

    // Wait for all runs to start
    boolean completed = latch.await(10, TimeUnit.SECONDS);
    assertTrue(completed, "Not all runs started within the expected time.");

    // Verify that all runs are in progress
    List<RunDetails> runDetailsList = runnerManager.getRunDetails();
    assertEquals(numRuns, runDetailsList.size(), "Unexpected number of runs in progress.");

    // Verify that each run is independent
    for (int i = 0; i < numRuns; i++) {
      String runId = "test-run-" + i;
      RunDetails details = runnerManager.getRunDetails(runId);
      assertNotNull(details, "RunDetails should not be null for runId: " + runId);
      assertEquals(runId, details.getRunId(), "Run ID mismatch.");
      assertFalse(details.isDone(), "Run should not be completed immediately.");
    }

    // log.info("All simultaneous runs started independently.");
  }

}
