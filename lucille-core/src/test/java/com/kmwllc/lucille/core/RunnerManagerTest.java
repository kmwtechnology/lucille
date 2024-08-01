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

    // While we are sleeping, ensure lucille isRunning and a new run is skipped
    assertTrue(runnerManager.isRunning());
    assertFalse(runnerManager.runWithConfig(config));

    // Sleep until the run is over
    runnerManager.waitForRunCompletion();

    // Ensure lucille is not running and make sure we can now kick off a new run
    assertFalse(runnerManager.isRunning());
    assertTrue(runnerManager.runWithConfig(config));
  }

  @Test
  public void testWaitForRunCompletion() throws InterruptedException, ExecutionException {
    RunnerManager runnerManager = RunnerManager.getInstance();
    Config config = ConfigFactory.load("RunnerManagerTest/sleep.conf");

    // Ensure the waitForCompletion method returns false if no lucille run is currently occurring
    assertFalse(runnerManager.waitForRunCompletion());

    runnerManager.runWithConfig(config);

    // Wait for the first run to finish, and then assert a run is no longer occurring
    assertTrue(runnerManager.waitForRunCompletion());
    assertFalse(runnerManager.waitForRunCompletion());
  }
}