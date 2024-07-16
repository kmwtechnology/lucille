package com.kmwllc.lucille.core;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import scala.Int;

public class RunnerManagerTest {

  @Test
  public void testRunnerManagerFull() throws InterruptedException {
    RunnerManager rm = RunnerManager.getInstance();
    Config config = ConfigFactory.load("RunnerManagerTest/sleep.conf");

    // Ensure no lucille run is running at the start of the test
    assertFalse(rm.isRunning());

    // Kick off a lucille run and ensure it is not skipped
    assertTrue(rm.runWithConfig(config));

    Thread.sleep(300);

    // While we are sleeping, ensure lucille isRunning and a new run is skipped
    assertTrue(rm.isRunning());
    assertFalse(rm.runWithConfig(config));

    // Sleep until the run is over
    Thread.sleep(1000);

    // Ensure lucille is not running and make sure we can now kick off a new run
    assertFalse(rm.isRunning());
    assertTrue(rm.runWithConfig(config));
  }

}
