package com.kmwllc.lucille.core;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.kmwllc.lucille.core.Runner.RunType;
import io.swagger.v3.oas.annotations.media.Schema;

@Schema(
    description = "Details about a specific run, including metadata, start time, and processing stats.")
public class RunDetails {

  private static final Logger log = LoggerFactory.getLogger(RunDetails.class);

  @Schema(description = "Unique identifier for the run.", example = "run123")
  private final String runId;

  @Schema(description = "Configuration ID for the run.")
  private String configId;

  @Schema(description = "Start time of the run in ISO-8601 format.",
      example = "2023-12-20T14:48:00.000Z")
  private final Instant startTime;

  @Schema(
      description = "End time of the run in ISO-8601 format. Null if the run has not completed.",
      example = "2023-12-20T15:48:00.000Z")
  private Instant endTime;

  @Schema(hidden = true)
  private transient CompletableFuture<Void> future;

  @Schema(description = "Run result")
  private RunResult runResult;

  @Schema(description = "Run type")
  private RunType runType;

  @Schema(description = "Exception of run")
  private Throwable throwable;


  public RunDetails(String runId, String configId) {
    this.runId = runId;
    this.configId = configId;
    this.startTime = Instant.now();
  }

  public String getConfigId() {
    return configId;
  }

  public void setConfigId(String configId) {
    this.configId = configId;
  }

  public String getRunId() {
    return runId;
  }

  public Instant getStartTime() {
    return startTime;
  }

  public Instant getEndTime() {
    return endTime;
  }

  public CompletableFuture<Void> getFuture() {
    return future;
  }

  public void setFuture(CompletableFuture<Void> future) {
    this.future = future;
  }

  public void setThrowable(Throwable ex) {
    this.endTime = Instant.now();
    this.throwable = ex;
  }

  public boolean isDone() {
    return (future != null && future.isDone());
  }

  public RunResult getRunResult() {
    return runResult;
  }

  public void setRunResult(RunResult runResult) {
    this.endTime = Instant.now();
    log.info("==completed run {} took {} ms==", runId, Duration.between(startTime, endTime));
    this.runResult = runResult;
  }

  public RunType getRunType() {
    return runType;
  }

  public void setRunType(RunType runType) {
    this.runType = runType;
  }
  
  public boolean hasThrowable() {
    return throwable != null;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("RunDetails{");
    sb.append("runId='").append(runId).append('\'');
    sb.append(", configId='").append(configId).append('\'');
    sb.append(", startTime=").append(startTime);
    sb.append(", endTime=").append(endTime);
    sb.append(", throwable=").append(throwable);
    sb.append(", runType=").append(runType);

    if (runResult != null) {
      sb.append(", runResult='").append(runResult).append('\'');
    }

    sb.append('}');
    return sb.toString();
  }
}
