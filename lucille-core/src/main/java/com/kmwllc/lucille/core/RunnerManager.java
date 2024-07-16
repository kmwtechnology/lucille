package com.kmwllc.lucille.core;

import com.kmwllc.lucille.core.Runner;
import com.kmwllc.lucille.core.Runner.RunType;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunnerManager {

  private static final Logger log = LoggerFactory.getLogger(RunnerManager.class);

  // TODO : Check if we need volatile keyword
  private static volatile RunnerManager instance = new RunnerManager();

  private RunnerManager() {}

  public static RunnerManager getInstance() {
    return instance;
  }

  synchronized public boolean isRunning() {
    return (future != null && !future.isDone());
  }

  private CompletableFuture<Void> future;

  /**
   * TODO : Fill in JAva doc
   * @return
   */
  synchronized public boolean run() {
    return runWithConfig(ConfigFactory.load());
  }

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
