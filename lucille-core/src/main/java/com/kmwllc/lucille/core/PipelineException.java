package com.kmwllc.lucille.core;

public class PipelineException extends Exception {

  public PipelineException(String message) {
    super(message);
  }

  public PipelineException(String message, Throwable cause) {
    super(message, cause);
  }

  public PipelineException() {
    super();
  }
}
