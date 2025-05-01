package com.kmwllc.lucille.objects;

import java.util.Objects;

/**
 * Represents the status of a Lucille job or process run.
 * <p>
 * Contains the run identifier and a flag indicating whether the process is running.
 */
public class RunStatus {

  /**
   * The identifier for the run. May be empty if not specified.
   */
  private final String runId;

  /**
   * Indicates if the process is currently running.
   */
  private boolean isRunning;

  /**
   * Constructs a RunStatus with the given run ID and running state.
   *
   * @param runId the identifier for the run
   * @param isRunning true if the process is running, false otherwise
   */
  public RunStatus(String runId, boolean isRunning) {
    this.runId = runId;
    this.isRunning = isRunning;
  }

  /**
   * Constructs a RunStatus with no run ID and the given running state.
   *
   * @param isRunning true if the process is running, false otherwise
   */
  public RunStatus(boolean isRunning) {
    this.runId = "";
    this.isRunning = isRunning;
  }

  /**
   * Returns the run identifier.
   *
   * @return the run ID, or empty string if not specified
   */
  public String getRunId() {
    return runId;
  }

  /**
   * Returns whether the process is currently running.
   *
   * @return true if running, false otherwise
   */
  public boolean isRunning() {
    return isRunning;
  }

  /**
   * Compares this RunStatus to another object for equality.
   *
   * @param o the object to compare
   * @return true if the objects are equal, false otherwise
   */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof RunStatus)) {
      return false;
    }

    RunStatus status = (RunStatus) o;

    return this.isRunning == status.isRunning && Objects.equals(this.runId, status.runId);
  }

  /**
   * Returns a string representation of this RunStatus.
   *
   * @return a string describing the running state and run ID
   */
  @Override
  public String toString() {
    return String.format("{'isRunning': '%s', 'runId': '%s'}", this.isRunning, this.runId);
  }

}
