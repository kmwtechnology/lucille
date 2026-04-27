package com.kmwllc.lucille.core;

/**
 * Thrown by an {@link Indexer} implementation when a batch send fails with an HTTP-level error
 * that may be transient and worth retrying. Carries the HTTP status code so that the base
 * {@link Indexer} can decide whether to retry based on the configured
 * {@code indexer.retryableStatusCodes} list.
 *
 * <p>Use {@link IndexerException} instead when the failure is not HTTP-based or should never
 * be retried regardless of configuration.
 */
public class IndexerRetryableException extends IndexerException {

  /** Sentinel value used when no HTTP status code is available (e.g. a pure network failure). */
  public static final int UNKNOWN_STATUS_CODE = -1;

  private final int statusCode;

  /**
   * Creates an exception with a known HTTP status code.
   *
   * @param statusCode the HTTP status code returned by the backend
   * @param message    a description of the failure
   * @param cause      the underlying exception
   */
  public IndexerRetryableException(int statusCode, String message, Throwable cause) {
    super(message, cause);
    this.statusCode = statusCode;
  }

  /**
   * Creates an exception for a non-HTTP failure (e.g. a network timeout) where no status code
   * is available. Uses {@value #UNKNOWN_STATUS_CODE} as the status code.
   *
   * @param message a description of the failure
   * @param cause   the underlying exception
   */
  public IndexerRetryableException(String message, Throwable cause) {
    super(message, cause);
    this.statusCode = UNKNOWN_STATUS_CODE;
  }

  /**
   * Returns the HTTP status code associated with this failure, or {@value #UNKNOWN_STATUS_CODE}
   * if no status code is available.
   *
   * @return the HTTP status code, or {@value #UNKNOWN_STATUS_CODE}
   */
  public int getStatusCode() {
    return statusCode;
  }
}
