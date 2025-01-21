package com.kmwllc.lucille.core;

/**
 * Exception for invalid configuration of a Lucille run.
 */
public class RunnerManagerException extends Exception {

  private static final long serialVersionUID = 1L;

  public RunnerManagerException(String message) {
    super(message);
  }

  public RunnerManagerException(String message, Throwable cause) {
    super(message, cause);
  }

  public RunnerManagerException(Throwable cause) {
    super(cause);
  }

  protected RunnerManagerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public RunnerManagerException() {
    super();
  }
}
