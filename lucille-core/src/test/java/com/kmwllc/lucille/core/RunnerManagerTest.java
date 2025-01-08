package com.kmwllc.lucille.core;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.Test;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class RunnerManagerTest {

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
}
