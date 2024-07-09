package com.kmwllc.lucille.objects;

public class RunStatus {

  private final String runId;

  private boolean isRunning;

  public RunStatus(String runId, boolean isRunning) {
    this.runId = runId;
    this.isRunning = isRunning;
  }

  public String getRunId() {
    return runId;
  }

  public boolean isRunning() {
    return isRunning;
  }

}
