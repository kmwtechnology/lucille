package com.kmwllc.lucille.core;

public class ConnectorException extends Exception {

  public ConnectorException(String message) {
    super(message);
  }

  public ConnectorException(String message, Throwable cause) {
    super(message, cause);
  }

  public ConnectorException(Throwable cause) {
    super(cause);
  }

  protected ConnectorException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public ConnectorException() {
    super();
  }
}
