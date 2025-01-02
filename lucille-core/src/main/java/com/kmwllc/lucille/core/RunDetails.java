package com.kmwllc.lucille.core;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.typesafe.config.Config;
import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "Details about a specific run, including metadata, start time, and processing stats.")
public class RunDetails {

    @Schema(description = "Unique identifier for the run.", example = "run123")
    protected final String runId;

    @Schema(description = "Start time of the run in ISO-8601 format.", example = "2023-12-20T14:48:00.000Z")
    protected final Instant startTime;

    @Schema(description = "End time of the run in ISO-8601 format. Null if the run has not completed.", example = "2023-12-20T15:48:00.000Z")
    protected Instant endTime;

    @Schema(description = "Number of documents processed during the run.", example = "5000")
    protected long docsProcessed;

    @Schema(description = "Number of errors encountered during the run.", example = "5")
    protected long errorCount;

    @Schema(description = "Configuration settings for the run.", example = "{\"batchSize\": 100, \"timeout\": 30}")
    protected Map<String, Object> config;

    private final transient CompletableFuture<Void> future;

    protected RunResult runResult = null;

    public RunDetails(String runId, Config config) {
        this.runId = runId;
        this.config = config.root().unwrapped();
        this.startTime = Instant.now();
        this.future = new CompletableFuture<>();
    }

    @Schema(description = "Get the configuration settings for the run.")
    public Map<String, Object> getConfig() {
        return config;
    }

    @Schema(description = "Set the configuration settings for the run.")
    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    @Schema(description = "Get the unique identifier for the run.")
    public String getRunId() {
        return runId;
    }

    @Schema(description = "Get the start time of the run.")
    public Instant getStartTime() {
        return startTime;
    }

    @Schema(description = "Get the end time of the run.")
    public Instant getEndTime() {
        return endTime;
    }

    public void setEndTime(Instant endTime) {
        this.endTime = endTime;
    }

    @Schema(description = "Get the number of documents processed during the run.")
    public long getDocsProcessed() {
        return docsProcessed;
    }

    public void setDocsProcessed(long docsProcessed) {
        this.docsProcessed = docsProcessed;
    }

    @Schema(description = "Get the number of errors encountered during the run.")
    public long getErrorCount() {
        return errorCount;
    }

    public void setErrorCount(long errorCount) {
        this.errorCount = errorCount;
    }

    @Schema(hidden = true)
    public CompletableFuture<Void> getFuture() {
        return future;
    }

    public void complete() {
        this.endTime = Instant.now();
        this.future.complete(null);
    }

    public void completeExceptionally(Throwable ex) {
        this.endTime = Instant.now();
        this.future.completeExceptionally(ex);
    }

    @Schema(description = "Check if the run is complete.", example = "true")
    public boolean isDone() {
        return future.isDone();
    }

    public RunResult getRunResult() {
        return runResult;
    }

    public void setRunResult(RunResult runResult) {
        this.runResult = runResult;
    }
    
}
