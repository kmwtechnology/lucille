package com.kmwllc.lucille.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Public API for starting lucille runs and viewing their status. Will be used by external
 * resources, namely the Admin API to kick off lucille runs.
 */
public class RunnerManager {

  private static final Logger log = LoggerFactory.getLogger(RunnerManager.class);

  // Singleton instance
  private static final RunnerManager instance = new RunnerManager();

  private final Map<String, RunDetails> runDetailsMap = new ConcurrentHashMap<>();

  private final Map<String, Config> configMap = new ConcurrentHashMap<>();

  private RunnerManager() {}

  public static RunnerManager getInstance() {
    return instance;
  }

  /**
   * Check whether a specific run is still in progress.
   *
   * @param runId the ID of the run to check
   * @return true if the run exists and is not completed; false otherwise
   */
  public synchronized boolean isRunning(String runId) {
    RunDetails details = runDetailsMap.get(runId);
    return details != null && !details.isDone();
  }

  /**
   * Waits for the specified run to complete.
   *
   * @param runId the ID of the run to wait for
   * @throws Exception if the run throws an exception or is interrupted
   */
  public void waitForRunCompletion(String runId) throws Exception {
    RunDetails details = runDetailsMap.get(runId);
    if (details != null) {
      details.getFuture().get();
    } else {
      log.error("No such run with ID: " + runId);
      return;
      // throw new IllegalArgumentException("No such run with ID: " + runId);
    }
  }

  /**
   * Starts a new lucille run with the given runId and default configuration. Will not start if a
   * run with the given runId is already in progress.
   *
   * @param runId the unique ID for the lucille run
   * @return true if the run was started successfully; false if a run with the given ID is already
   *         in progress
   */
  public synchronized boolean run(String runId) {
    Config config = ConfigFactory.load();
    String configId = createConfig(config);
    return runWithConfig(runId, configId);
  }

  /**
   * Internal runId
   * 
   * @param config
   * @return
   */
  protected synchronized boolean runWithConfig(Config config) {
    String configId = createConfig(config);
    return runWithConfig(null, configId);
  }

  /**
   * Method to create a lucille run with a custom configuration.
   *
   * @param runId the unique ID for the lucille run
   * @param config the configuration to use for the run
   * @return true if the run was started successfully; false if a run with the given ID is already
   *         in progress
   */
  public synchronized String createConfig(Config config) {
    String configId = UUID.randomUUID().toString();
    configMap.put(configId, config);
    return configId;
  }

  /**
   * Method to start a lucille run with a custom configuration.
   *
   * @param runId the unique ID for the lucille run
   * @param config the configuration to use for the run
   * @return true if the run was started successfully; false if a run with the given ID is already
   *         in progress
   */
  public synchronized boolean runWithConfig(String inRunId, String configId) {
    final String runId = (inRunId != null) ? inRunId : UUID.randomUUID().toString();

    if (isRunning(runId)) {
      log.warn("Skipping new run with ID '{}'; previous lucille run is still in progress.", runId);
      return false;
    }

    final RunDetails runDetails = new RunDetails(runId, configId);
    runDetailsMap.put(runId, runDetails);

    CompletableFuture.runAsync(() -> {
      try {
        log.info("Starting lucille run with ID '{}' via the Runner Manager using configId {}.",
            runId, configId);

        // For now, we will always use local mode without Kafka
        runDetails.setRunType(Runner.getRunType(false, true));

        Config config = configMap.get(configId);
        if (config == null) {
          log.error("Config with id {} not found", configId);
          throw new Exception("Config with id " + configId + " not found");
        }
        log.info(config.entrySet().toString());

        runDetails.setRunResult(Runner.runWithResultLog(config, runDetails.getRunType(), runId));
        runDetails.complete();
      } catch (Exception e) {
        log.error("Failed to run lucille with ID '{}' via the Runner Manager.", runId, e);
        runDetails.completeExceptionally(e);
      } finally {
        // optionally remove from memory
        // runDetailsMap.remove(runId);
        log.info("finished run with id {}", runId);
      }
    });

    return true;
  }

  /**
   * Retrieves the details of a specific run.
   *
   * @param runId the ID of the run
   * @return the RunDetails object, or null if no such run exists
   */
  public RunDetails getRunDetails(String runId) {
    return runDetailsMap.get(runId);
  }

  /**
   * List of details of run
   * 
   * @return
   */
  public List<RunDetails> getRunDetailsList() {
    return new ArrayList<>(runDetailsMap.values());
  }

  public Map<String, RunDetails> getRunDetailsMap() {
    return runDetailsMap;
  }

  public Map<String, Config> getConfigMap() {
    return configMap;
  }

  public Config getConfig(String configId) {
    return configMap.get(configId);
  }
}
