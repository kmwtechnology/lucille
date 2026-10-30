package com.kmwllc.lucille.core;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.kmwllc.lucille.core.Runner.RunType;

/**
 * Details about a run started by the {@link RunnerManager}. Static access only, since only the RunnerManager
 * creates and updates RunDetails.
 */
public class RunDetails {

  private static final Logger log = LoggerFactory.getLogger(RunDetails.class);

  private final String runId;
  private final String configId;
  private final Instant startTime;

  private Instant endTime;
  private CompletableFuture<Void> future;
  private RunResult runResult;
  private RunType runType;
  private Throwable throwable;

  RunDetails(String runId, String configId) {
    this.runId = runId;
    this.configId = configId;
    this.startTime = Instant.now();
  }

  public String getConfigId() {
    return configId;
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

  CompletableFuture<Void> getFuture() {
    return future;
  }

  void setFuture(CompletableFuture<Void> future) {
    this.future = future;
  }

  void setThrowable(Throwable ex) {
    this.endTime = Instant.now();
    this.throwable = ex;
  }

  public boolean isDone() {
    return (future != null && future.isDone());
  }

  public RunResult getRunResult() {
    return runResult;
  }

  void setRunResult(RunResult runResult) {
    this.endTime = Instant.now();
    log.info("==completed run {} took {} ms==", runId, Duration.between(startTime, endTime));
    this.runResult = runResult;
  }

  public RunType getRunType() {
    return runType;
  }

  void setRunType(RunType runType) {
    this.runType = runType;
  }

  public boolean hasThrowable() {
    return throwable != null;
  }

  /**
   * @return The Throwable that ended this run before it produced a RunResult, or null if there was none.
   */
  // never serialized: it would expose the full stack trace and cause chain. The API reports it as a RunError.
  @JsonIgnore
  public Throwable getThrowable() {
    return throwable;
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
