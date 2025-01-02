package com.kmwllc.lucille.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kmwllc.lucille.core.Runner.RunType;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Public API for starting lucille runs and viewing their status. Will be used by external resources, namely the Admin API to kick
 * off lucille runs.
 */
public class RunnerManager {

    private static final Logger log = LoggerFactory.getLogger(RunnerManager.class);

    // Singleton instance
    private static final RunnerManager instance = new RunnerManager();

    private final Map<String, RunDetails> runStatusMap = new ConcurrentHashMap<>();

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
        RunDetails details = runStatusMap.get(runId);
        return details != null && !details.isDone();
    }

    /**
     * Waits for the specified run to complete.
     *
     * @param runId the ID of the run to wait for
     * @throws Exception if the run throws an exception or is interrupted
     */
    public void waitForRunCompletion(String runId) throws Exception {
        RunDetails details = runStatusMap.get(runId);
        if (details != null) {
            details.getFuture().get();
        } else {
            throw new IllegalArgumentException("No such run with ID: " + runId);
        }
    }

    /**
     * Starts a new lucille run with the given runId and default configuration. Will not start if a run with the given runId is
     * already in progress.
     *
     * @param runId the unique ID for the lucille run
     * @return true if the run was started successfully; false if a run with the given ID is already in progress
     */
    public synchronized boolean run(String runId) {
        return runWithConfig(runId, ConfigFactory.load());
    }
    
    /**
     * Internal runId
     * @param config
     * @return
     */
    protected synchronized boolean runWithConfig(Config config) {
    	return runWithConfig(null, config);
    }

    /**
     * Internal method to start a lucille run with a custom configuration. Supports testing.
     *
     * @param runId the unique ID for the lucille run
     * @param config the configuration to use for the run
     * @return true if the run was started successfully; false if a run with the given ID is already in progress
     */
    public synchronized boolean runWithConfig(String runId, Config config) {
    	if (runId ==  null) {
    		runId = UUID.randomUUID().toString();
    	}
    	
    	final String finalRunId = runId;
    	
        if (isRunning(runId)) {
            log.warn("Skipping new run with ID '{}'; previous lucille run is still in progress.", runId);
            return false;
        }

        RunDetails runDetails = new RunDetails(runId, config);
        runStatusMap.put(runId, runDetails);

        CompletableFuture.runAsync(() -> {
            try {
                log.info("Starting lucille run with ID '{}' via the Runner Manager.", finalRunId);
                log.info(config.entrySet().toString());

                // For now, we will always use local mode without Kafka
                RunType runType = Runner.getRunType(false, true);

                RunResult runResult = Runner.runWithResultLog(config, runType);
                runDetails.setRunResult(runResult);
                runDetails.complete();
            } catch (Exception e) {
                log.error("Failed to run lucille with ID '{}' via the Runner Manager.", finalRunId, e);
                runDetails.setErrorCount(runDetails.getErrorCount() + 1);
                runDetails.completeExceptionally(e);
            } finally {
                // optionally remove from memory
                // runDetailsMap.remove(runId);
            	log.info("finished run with id {}", finalRunId);
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
    public RunDetails getStatus(String runId) {
        return runStatusMap.get(runId);
    }

    /**
     * List of details of run
     * @return
     */
	public List<RunDetails> getStatusList() {		
		return new ArrayList<>(runStatusMap.values());
	}
}
