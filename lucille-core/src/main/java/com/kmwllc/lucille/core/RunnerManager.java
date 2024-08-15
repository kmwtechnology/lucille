package com.kmwllc.lucille.core;

import com.kmwllc.lucille.core.Runner;
import com.kmwllc.lucille.core.Runner.RunType;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Public API for starting lucille runs and viewing their status. Will be used by external resources, namely the Admin API to kick
 * off lucille runs.
 */
public class RunnerManager {

  private static final Logger log = LoggerFactory.getLogger(RunnerManager.class);

  // Use the eager-initialization singleton pattern
  private static volatile RunnerManager instance = new RunnerManager();

  private RunnerManager() {}

  public static RunnerManager getInstance() {
    return instance;
  }

  // Check whether the CompleteableFuture exists and is not done
  synchronized public boolean isRunning() {
    return !future.isDone();
  }

  /**
   * Blocks the calling thread until the current lucille run is completed. This method is primarily intended to be used for testing,
   * but has been made public to facilitate testing in other modules, namely lucille-plugins/lucille-api.
   */
  public void waitForRunCompletion() throws ExecutionException, InterruptedException {
    future.get();
  }

  private CompletableFuture<Void> future = CompletableFuture.completedFuture(null);

  /**
   * Main entrypoint for kicking off lucille runs. This method spawns a new thread for lucille to run in, but will not start a new
   * instance of lucille until the previous one terminates.
   *
   * @return boolean representing whether the lucille run was initiated or not. This will return false if and only if the lucille
   * run was skipped due to the previous run still existing.
   */
  synchronized public boolean run() {
    return runWithConfig(ConfigFactory.load());
  }

  /**
   * Internal abstraction used to support testing.
   */
  synchronized protected boolean runWithConfig(Config config) {
    if (isRunning()) {
      log.warn("Skipping new run; previous lucille run is still in progress.");
      return false;
    }

    future = CompletableFuture.runAsync(() -> {
      try {
        log.info("Starting lucille run via the Runner Manager.");
        log.info(config.entrySet().toString());

        // For now we will always use local mode without kafka
        RunType runType = Runner.getRunType(false, true);

        Runner.runWithResultLog(config, runType);
      } catch (Exception e) {
        log.error("Failed to run lucille via the Runner Manager.", e);
      }
    });

    return true;
  }
}
