package com.kmwllc.lucille.core;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.concurrent.ExecutionException;
import org.junit.Before;
import org.junit.Test;
import scala.Int;

public class RunnerManagerTest {

  @Test
  public void testRunnerManagerFull() throws InterruptedException, ExecutionException {
    RunnerManager runnerManager = RunnerManager.getInstance();
    Config config = ConfigFactory.load("RunnerManagerTest/sleep.conf");

    // Ensure no lucille run is running at the start of the test
    assertFalse(runnerManager.isRunning());

    // Kick off a lucille run and ensure it is not skipped
    assertTrue(runnerManager.runWithConfig(config));

    // While we lucille is running, ensure lucille isRunning and a new run is skipped
    assertTrue(runnerManager.isRunning());
    assertFalse(runnerManager.runWithConfig(config));

    // Wait until the run is over
    runnerManager.waitForRunCompletion();

    // Ensure lucille is not running and make sure we can now kick off a new run
    assertFalse(runnerManager.isRunning());
    assertTrue(runnerManager.runWithConfig(config));

    // Wait for all lucille threads to finish before exiting
    runnerManager.waitForRunCompletion();
  }

  @Test
  public void testWaitForRunCompletion() throws InterruptedException, ExecutionException {
    RunnerManager runnerManager = RunnerManager.getInstance();
    Config config = ConfigFactory.load("RunnerManagerTest/sleep.conf");

    // Ensure lucille is not running first
    assertFalse(runnerManager.isRunning());

    runnerManager.runWithConfig(config);

    // Ensure lucille is running, wait for it stop and ensure its stopped
    assertTrue(runnerManager.isRunning());
    runnerManager.waitForRunCompletion();
    assertFalse(runnerManager.isRunning());
  }
}
