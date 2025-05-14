package com.kmwllc.lucille.core;

public class StageException extends BaseConfigException {

  public StageException(BaseConfigException e) {
    super(e.getMessage());
  }

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
