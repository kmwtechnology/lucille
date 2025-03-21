package com.kmwllc.lucille.core;

/**
 * An Exception thrown by a Connector.
 */
public class ConnectorException extends Exception {

  /**
   * Creates a ConnectorException with the given message.
   * @param message The message associated with the ConnectorException.
   */
  public ConnectorException(String message) {
    super(message);
  }

  /**
   * Creates a ConnectorException with the given message and Throwable.
   * @param message The message associated with the ConnectorException.
   * @param cause A Throwable that caused the ConnectorException.
   */
  public ConnectorException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates a ConnectorException with the given Throwable as a cause.
   * @param cause A Throwable that caused the ConnectorException.
   */
  public ConnectorException(Throwable cause) {
    super(cause);
  }

  /**
   * Creates a ConnectorException with the given message and Throwable cause. Uses the given settings to enable / disable
   * suppression of the Exception and whether the stack trace is writable.
   * @param message The message associated with the Exception.
   * @param cause A Throwable that caused the ConnectorException.
   * @param enableSuppression Whether the Exception can be suppressed.
   * @param writableStackTrace Whether the stack trace is writable.
   */
  protected ConnectorException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  /**
   * Creates a ConnectorException.
   */
  public ConnectorException() {
    super();
  }
}
