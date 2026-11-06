package com.kmwllc.lucille.api.objects;

import com.kmwllc.lucille.core.RunDetails;
import com.kmwllc.lucille.core.RunResult;
import com.kmwllc.lucille.core.Runner.RunType;
import io.swagger.v3.oas.annotations.media.Schema;
import java.time.Instant;

/**
 * The API's representation of a run: an immutable snapshot of a {@link RunDetails}, taken when the response is built.
 */
@Schema(
    description = "Details about a specific run, including metadata, start time, and processing stats.")
public class RunDetailsResponse {

  @Schema(description = "Unique identifier for the run.", example = "run123")
  private final String runId;

  @Schema(description = "Configuration ID for the run.")
  private final String configId;

  @Schema(description = "Start time of the run in ISO-8601 format.",
      example = "2023-12-20T14:48:00.000Z")
  private final Instant startTime;

  @Schema(
      description = "End time of the run in ISO-8601 format. Null if the run has not completed.",
      example = "2023-12-20T15:48:00.000Z")
  private final Instant endTime;

  @Schema(description = "Run result. Null while the run is in progress, or if it failed before producing one.")
  private final RunResult runResult;

  @Schema(description = "Run type")
  private final RunType runType;

  @Schema(description = "Whether the run has finished, successfully or not.")
  private final boolean done;

  @Schema(description = "Why the run failed, if it failed before producing a run result. Null otherwise.")
  private final RunError error;

  private RunDetailsResponse(RunDetails details) {
    // read done first: a run's details are all set before its future completes, so if done is true, the reads
    // below see the finished run rather than a mix of in-progress and finished state
    this.done = details.isDone();
    this.runId = details.getRunId();
    this.configId = details.getConfigId();
    this.startTime = details.getStartTime();
    this.endTime = details.getEndTime();
    this.runResult = details.getRunResult();
    this.runType = details.getRunType();
    this.error = RunError.from(details.getThrowable());
  }

  /**
   * @param details The RunDetails to snapshot, or null.
   * @return A snapshot of <code>details</code>, or null if it is null.
   */
  public static RunDetailsResponse from(RunDetails details) {
    return details == null ? null : new RunDetailsResponse(details);
  }

  public String getRunId() {
    return runId;
  }

  public String getConfigId() {
    return configId;
  }

  public Instant getStartTime() {
    return startTime;
  }

  public Instant getEndTime() {
    return endTime;
  }

  public RunResult getRunResult() {
    return runResult;
  }

  public RunType getRunType() {
    return runType;
  }

  public boolean isDone() {
    return done;
  }

  public RunError getError() {
    return error;
  }
}
