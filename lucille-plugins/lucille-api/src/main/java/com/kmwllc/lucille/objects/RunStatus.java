package com.kmwllc.lucille.objects;

public class RunStatus {

  private final String runId;

  private boolean isRunning;

  public RunStatus(String runId, boolean isRunning) {
    this.runId = runId;
    this.isRunning = isRunning;
  }

  public RunStatus(boolean isRunning) {
    this.runId = "";
    this.isRunning = isRunning;
  }

  public String getRunId() {
    return runId;
  }

  public boolean isRunning() {
    return isRunning;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof RunStatus)) {
      return false;
    }

    RunStatus status = (RunStatus) o;

    return this.isRunning == status.isRunning && this.runId.equals(status.runId);
  }

  @Override
  public String toString() {
    return String.format("{'isRunning': '%s', 'runId': '%s'}", this.isRunning, this.runId);
  }

}
