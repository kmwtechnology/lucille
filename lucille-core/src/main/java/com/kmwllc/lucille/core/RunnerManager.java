package com.kmwllc.lucille.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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

  // value will get reset to this post unit testing.
  private static final int DEFAULT_MAX_HISTORY = 10000;
  private static final int DEFAULT_MAX_CONFIGS = 10000;

  private static final Logger log = LoggerFactory.getLogger(RunnerManager.class);

  // Singleton instance
  private static final RunnerManager instance = new RunnerManager();

  // may be changed for unit testing. default should be DEFAULT_MAX_HISTORY,
  // which is used elsewhere.
  private int maxHistory = DEFAULT_MAX_HISTORY;
  private int maxConfigs = DEFAULT_MAX_CONFIGS;

  // RunID keyed maps, the first is for active runs, the second is for historical runs.
  // We do this to optionally allow for a limited number of historical runs to be stored.
  // Some invariants regarding these maps and their effects:
  // - RunDetails are only added to the historical details map AFTER the run "isDone".
  //   - Therefore, all RunDetails in the historicalDetailsMap should have isDone == true
  //   - To retrieve RunDetails, check the active map *and then* the historical map - the order matters!
  // - RunDetails in the activeDetailsMap will (very briefly) have isDone == true
  //   - The presence of a runID in the activeDetailsMap alone is *not* enough to completely assert that
  //     a run is actually in progress
  // - RunDetails may (briefly) be present in both maps.
  // - Reads/Writes to either of these maps must be protected by synchronizing with the singleton instance.
  private final Map<String, RunDetails> activeDetailsMap = new HashMap<>();

  private final Map<String, RunDetails> historicalDetailsMap =
      new LinkedHashMap<>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
          return size() > maxHistory;
        }
      };

  // An in memory map of configs which can be used to create new Lucille runs
  private final Map<String, Config> configMap = new LinkedHashMap<>();

  // Mapping Config IDs currently in use to the associated Run ID. Only used when
  // we aim to prevent concurrent runs of the same config. Config IDs are only
  // present in this map when they are in use.
  private final Map<String, String> lockedConfigMap = new ConcurrentHashMap<>();

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
    RunDetails details = activeDetailsMap.get(runId);
    return details != null && !details.isDone();
  }

  /**
   * Waits for the specified run to complete.
   *
   * @param runId the ID of the run to wait for
   * @throws Exception if the run throws an exception or is interrupted
   */
  public void waitForRunCompletion(String runId) throws Exception {
    RunDetails details = getRunDetails(runId);

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
    String configId = createConfig(config).getConfigId();
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
    String configId = createConfig(config).getConfigId();
    return runWithConfig(Runner.generateRunId(), configId);
  }

  /**
   * Method to create a lucille run with a custom configuration.
   *
   * @param config the configuration to use for the run
   * @return The UUID String associated with the supplied config in the config map.
   */
  public synchronized CreateConfigResult createConfig(Config config) {
    String configId = UUID.randomUUID().toString();
    configMap.put(configId, config);

    if (configMap.size() > maxConfigs) {
      String configIdToRemove = configMap.keySet().iterator().next();
      configMap.remove(configIdToRemove);
      return new CreateConfigResult(configId, configIdToRemove);
    }

    return new CreateConfigResult(configId);
  }

  /**
   * Adds the provided Config keyed under the provided name. Returns whether the config
   * was stored successfully. This method will <i>not</i> overwrite an existing config
   * with the same name.
   * @param config The configuration to store.
   * @param name The name under which the configuration should be keyed / referenced.
   * @return Whether the config was successfully stored and associated with the provided name.
   */
  public synchronized boolean createConfigWithName(Config config, String name) {
    if (configMap.containsKey(name)) {
      log.warn("Attempted to add config with name {}, which already exists", name);
      return false;
    }

    configMap.put(name, config);
    return true;
  }

  /**
   * Removes a Config by ID and returns whether it was present and removed successfully.
   * @param configId The ID of the Config to delete.
   * @return Whether a config with the provided ID was present in the map and removed successfully.
   */
  public synchronized boolean deleteConfig(String configId) {
    if (!configMap.containsKey(configId)) {
      return false;
    }

    configMap.remove(configId);
    return true;
  }

  /**
   * Method to start a lucille run with a custom configuration. Config will not be locked.
   *
   * @param runId the unique ID for the lucille run
   * @param configId the ID of the config you want to use for the run.
   * @return RunDetails
   * @throws RunnerManagerException
   */
  public synchronized RunDetails runWithConfig(String runId, String configId) throws RunnerManagerException {
    return runWithConfig(runId, configId, false);
  }

  /**
   * Method to start a lucille run with a custom configuration.
   *
   * @param runId the unique ID for the lucille run
   * @param configId the ID of the config you want to use for the run.
   * @param lockConfig Whether you want to prevent future runs from using this configId until this run is resolved.
   * @return RunDetails
   * @throws RunnerManagerException For invalid arguments or if the configId is currently locked (regardless of <code>lockConfig</code>'s value).
   */
  public synchronized RunDetails runWithConfig(String runId, String configId, boolean lockConfig)
      throws RunnerManagerException {

    if (runId == null) {
      throw new RunnerManagerException("runId cannot be null");
    }
    
    if (activeDetailsMap.containsKey(runId) || historicalDetailsMap.containsKey(runId)) {
      throw new RunnerManagerException("Run with runId " + runId + " already defined - cannot use this runId again");
    }

    final Config config = configMap.get(configId);
    if (config == null) {
      throw new RunnerManagerException("Config with id " + configId + " not found");
    }

    if (lockedConfigMap.containsKey(configId)) {
      throw new RunnerManagerException("configId " + configId + " is locked, in use by run " + lockedConfigMap.get(configId));
    }

    final RunDetails runDetails = new RunDetails(runId, configId);
    activeDetailsMap.put(runId, runDetails);

    if (lockConfig) {
      lockedConfigMap.put(configId, runId);
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
        runDetails.setThrowable(e);
      } finally {
        if (lockConfig) {
          lockedConfigMap.remove(configId, runId);
        }
        
        synchronized (instance) {
          // overriding removeEldestEntry() above means run details
          // beyond the limit are removed automatically
          historicalDetailsMap.put(runId, runDetails);
          activeDetailsMap.remove(runId);
        }
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
  public synchronized RunDetails getRunDetails(String runId) {
    // Checking in the order as specified above.
    RunDetails activeDetails = activeDetailsMap.get(runId);

    if (activeDetails == null) {
      return historicalDetailsMap.get(runId);
    } else {
      return activeDetails;
    }
  }

  /**
   * List of details of all runs
   * 
   * @return
   */
  public synchronized List<RunDetails> getRunDetails() {
    // We have potential duplicates - runs may be in both maps briefly -
    // but we keep the same RunDetails instance, which the set can still consolidate.
    Set<RunDetails> results = new HashSet<>(activeDetailsMap.values());
    results.addAll(historicalDetailsMap.values());
    return new ArrayList<>(results);
  }

  /**
   * Returns the key set for all configs
   * 
   * @return
   */
  public synchronized Set<String> getConfigKeys() {
    return configMap.keySet();
  }

  public synchronized Config getConfig(String configId) {
    return configMap.get(configId);
  }

  /**
   * Modify the maximum number of historical runs that will be stored for testing purposes.
   * Resets the run history.
   * Package access for unit testing. This method is not thread-safe.
   */
  static void limitHistoriesForTesting(int maxHistory) {
    instance.historicalDetailsMap.clear();
    instance.maxHistory = maxHistory;

    instance.configMap.clear();
    instance.maxConfigs = maxHistory;
  }

  /**
   * Resets the maximum number of historical runs that will be stored to the default.
   * Package access for unit testing. This method is not thread-safe.
   */
  static void resetMaxHistoriesForTesting() {
    instance.maxHistory = DEFAULT_MAX_HISTORY;
    instance.maxConfigs = DEFAULT_MAX_CONFIGS;
  }
}
