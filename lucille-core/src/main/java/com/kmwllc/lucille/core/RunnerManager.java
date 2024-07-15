package com.kmwllc.lucille;

import com.kmwllc.lucille.core.Runner;
import com.kmwllc.lucille.core.Runner.RunType;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunnerManager {

  private static final Logger log = LoggerFactory.getLogger(RunnerManager.class);

  private static volatile RunnerManager instance = new RunnerManager();

  protected static boolean isRunning = false;

  private RunnerManager() {}

  public static RunnerManager getInstance() {
    return instance;
  }

  public boolean isRunning() {
    return isRunning;
  }

  private CompletableFuture<Void> future;

  synchronized public void run(boolean local) {
    if (future != null && !future.isDone()) {
      log.warn("Skipping new run; previous lucille run is still in progress.");
      return;
    }

    future = CompletableFuture.runAsync(() -> {
      try {
        log.info("Starting lucille run via the Runner Manager.");
        Config config = ConfigFactory.load();
        log.info(config.entrySet().toString());

        isRunning = true;

        // For now we will never use kafka
        RunType runType = Runner.getRunType(false, local);

        Runner.runWithResultLog(config, runType);
      } catch (Exception e) {
        log.error("Failed to run lucille via the Runner Manager.", e);
      } finally {
        isRunning = false;
      }
    });
  }
}
