package com.kmwllc.lucille.core;

public class ConnectorException extends Exception {
  public ConnectorException(String message) {
    super(message);
  }

  public ConnectorException(String message, Throwable cause) {
    super(message, cause);
  }

  public ConnectorException() {
    super();
  }
}
