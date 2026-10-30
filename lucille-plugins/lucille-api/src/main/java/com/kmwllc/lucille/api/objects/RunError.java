package com.kmwllc.lucille.api.objects;

import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Describes the Throwable that ended a run before it produced a RunResult, e.g. a connector that could not be
 * constructed.
 */
@Schema(description = "Why a run failed before producing a run result.")
public class RunError {

  @Schema(description = "Class of the exception that ended the run.",
      example = "java.lang.reflect.InvocationTargetException")
  private final String className;

  @Schema(description = "Message of the exception that ended the run. May be null.")
  private final String message;

  @Schema(description = "Class of the innermost cause. Same as className when the exception has no cause.",
      example = "com.kmwllc.lucille.core.ConnectorException")
  private final String rootCauseClassName;

  @Schema(description = "Message of the innermost cause. May be null.")
  private final String rootCauseMessage;

  private RunError(Throwable t, Throwable rootCause) {
    this.className = t.getClass().getName();
    this.message = t.getMessage();
    this.rootCauseClassName = rootCause.getClass().getName();
    this.rootCauseMessage = rootCause.getMessage();
  }

  /**
   * @param t The Throwable that ended a run, or null.
   * @return A RunError describing the Throwable, or null if <code>t</code> is null.
   */
  public static RunError from(Throwable t) {
    if (t == null) {
      return null;
    }

    // bounded, in case of a cycle in the cause chain
    Throwable rootCause = t;
    for (int depth = 0; rootCause.getCause() != null && rootCause.getCause() != rootCause && depth < 100; depth++) {
      rootCause = rootCause.getCause();
    }

    return new RunError(t, rootCause);
  }

  public String getClassName() {
    return className;
  }

  public String getMessage() {
    return message;
  }

  public String getRootCauseClassName() {
    return rootCauseClassName;
  }

  public String getRootCauseMessage() {
    return rootCauseMessage;
  }
}
