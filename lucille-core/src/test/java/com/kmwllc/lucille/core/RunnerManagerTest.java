package com.kmwllc.lucille.core;


import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
    log.info("======== testSimultaneousRuns begin ========");

    int numRuns = 5;
    log.info("Starting testSimultaneousRuns with {} runs", numRuns);

    CountDownLatch latch = new CountDownLatch(numRuns);
    RunnerManager runnerManager = RunnerManager.getInstance();
    Random random = new Random();


    // Configuration setup timing
    long configStartTime = System.nanoTime();
    String config = "connectors: [\n"
        + "    {class: \"com.kmwllc.lucille.connector.SleepConnector\", name: \"connector1\", duration: 5000, pipeline: \"pipeline1\"}\n"
        + "]\n" + "pipelines: [{name: \"pipeline1\", stages: []}]\n" + "indexer {\n"
        + "  sendEnabled: false\n" + "}\n";
    Config c = ConfigFactory.parseString(config);
    String configId = runnerManager.createConfig(c);
    long configEndTime = System.nanoTime();
    log.info("Configuration setup completed in {} ms",
        (configEndTime - configStartTime) / 1_000_000);

    long runStartTime = System.nanoTime();

    for (int i = 0; i < numRuns; i++) {
      final String runId = "test-run-" + i;
      // force random delay between threads to guarantee varied interleave
      Thread.sleep(random.nextInt(3000));
      new Thread(() -> {
        long individualRunStart = System.nanoTime();
        try {
          log.info("[{}] Starting run...", runId);
          RunDetails details = runnerManager.runWithConfig(runId, configId);
          boolean started = (details.getErrorCount() == 0);

          if (!started) {
            log.warn("[{}] Could not start; skipping.", runId);
          } else {
            log.info("[{}] Run started successfully.", runId);

            // Interleave checking: monitor while waiting for completion
            while (!runnerManager.getRunDetails(runId).isDone()) {
              Thread.sleep(500); // Poll every 500ms
            }

            long individualRunEnd = System.nanoTime();
            long runDuration = (individualRunEnd - individualRunStart) / 1_000_000;

            RunDetails detail = runnerManager.getRunDetails(runId);

            assertNotNull(detail, () -> "[%s] RunDetails should not be null.".formatted(runId));
            assertEquals(runId, detail.getRunId(), () -> "[%s] Run ID mismatch.".formatted(runId));
            assertEquals(0, detail.getErrorCount(),
                () -> "[%s] Run should complete without errors.".formatted(runId));

            log.info("[{}] Finished in {} ms with {} errors.", runId, runDuration,
                detail.getErrorCount());
          }
        } catch (Exception e) {
          log.error("[{}] Error during execution.", runId, e);
        } finally {
          latch.countDown();
        }
      }).start();
    }

    // Ensure all runs complete within the 15-second limit
    boolean completed = latch.await(10, TimeUnit.SECONDS);
    long runEndTime = System.nanoTime();
    long totalTestDuration = (runEndTime - runStartTime) / 1_000_000;

    log.info("All runs completed: {} (Test duration: {} ms)", completed, totalTestDuration);
    assertTrue(completed, "Not all runs completed within the expected time.");

    log.info("======== testSimultaneousRuns end ========");
  }


}
