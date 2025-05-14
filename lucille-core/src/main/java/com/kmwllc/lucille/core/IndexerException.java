package com.kmwllc.lucille.core;

public class IndexerException extends BaseConfigException {

  public IndexerException(BaseConfigException e) {
    super(e.getMessage());
  }

  public IndexerException(String message) {
    super(message);
  }

  public IndexerException(String message, Throwable cause) {
    super(message, cause);
  }

  public IndexerException() {
    super();
  }
}
