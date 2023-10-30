package com.kmwllc.lucille.core;

public class StageException extends Exception {

  public StageException(String message) {
    super(message);
  }

  public StageException(String message, Throwable cause) {
    super(message, cause);
  }

  public StageException() {
    super();
  }
}
