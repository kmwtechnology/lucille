package com.kmwllc.lucille.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  // A runID keyed map containing run details of current and historical Lucille runs
  private final Map<String, RunDetails> runDetailsMap = new ConcurrentHashMap<>();

  // An in memory map of configs which can be used to create new Lucille runs
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
    return details != null && details.getFuture() != null && !details.isDone();
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
      throw new IllegalArgumentException("No such run with ID: " + runId);
    }
  }

  /**
   * Starts a new lucille run with the given runId and default configuration. Will not start if a
   * run with the given runId is already in progress.
   *
   * @param runId the unique ID for the lucille run
   * @return RunDetails
   * @throws RunnerManagerException
   */
  public synchronized RunDetails run(String runId) throws RunnerManagerException {
    Config config = ConfigFactory.load();
    String configId = createConfig(config);
    return runWithConfig(runId, configId);
  }

  /**
   * Run with config and an internal runId will be generated
   * 
   * @param config
   * @return RunDetails
   * @throws RunnerManagerException
   */
  protected synchronized RunDetails runWithConfig(Config config) throws RunnerManagerException {
    String configId = createConfig(config);
    return runWithConfig(Runner.generateRunId(), configId);
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
   * @return RunDetails
   * @throws RunnerManagerException
   */
  public synchronized RunDetails runWithConfig(String runId, String configId)
      throws RunnerManagerException {

    if (runId == null) {
      throw new RunnerManagerException("runId cannot be null");
    }
    
    if (runDetailsMap.containsKey(runId)) {
      throw new RunnerManagerException("Run with runId " + runId + " already defined - cannot use this runId again");
    }

    final RunDetails runDetails = new RunDetails(runId, configId);
    runDetailsMap.put(runId, runDetails);

    if (isRunning(runId)) {
      log.warn("Skipping new run with ID '{}'; previous lucille run is still in progress.", runId);
      return runDetails;
    }

    final Config config = configMap.get(configId);
    if (config == null) {
      log.error("Config with id {} not found", configId);
      RunnerManagerException exception = new RunnerManagerException("Config with id " + configId + " not found");
      runDetails.setError(exception);
      throw exception;
    }

    runDetails.setFuture(CompletableFuture.runAsync(() -> {
      try {
        log.info("Starting lucille run with ID '{}' via the Runner Manager using configId {}.",
            runId, configId);

        // For now, we will always use local mode without Kafka
        runDetails.setRunType(Runner.getRunType(false, true));


        log.debug(config.entrySet().toString());

        runDetails.setRunResult(Runner.runWithResultLog(config, runDetails.getRunType(), runId));
      } catch (Exception e) {
        log.error("Failed to run lucille with ID '{}' via the Runner Manager.", runId, e);
        runDetails.setError(e);
      } finally {
        // optionally remove from memory
        // runDetailsMap.remove(runId);
        log.info("finished run with id {}", runId);
      }
    }));

    return runDetails;
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
   * List of details of all runs
   * 
   * @return
   */
  public List<RunDetails> getRunDetails() {
    return new ArrayList<>(runDetailsMap.values());
  }

  /**
   * Clears all run details
   * 
   * @return
   */
  public void clearAllRunDetails() {
    runDetailsMap.clear();
  }

  /**
   * Removes all config
   */
  public void clearConfig() {
    configMap.clear();
  }

  /**
   * Returns the key set for all configs
   * 
   * @return
   */
  public Set<String> getConfigKeys() {
    return configMap.keySet();
  }

  public Config getConfig(String configId) {
    return configMap.get(configId);
  }
}
