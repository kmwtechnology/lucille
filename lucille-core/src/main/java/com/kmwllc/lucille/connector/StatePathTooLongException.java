package com.kmwllc.lucille.connector;

/**
 * Thrown when a file path is longer than the state table's name column, so the file cannot be tracked. Escapes the
 * per-file error handling and fails the traversal, since every such file would otherwise be republished on every run.
 */
public class StatePathTooLongException extends RuntimeException {

  public StatePathTooLongException(String message, Throwable cause) {
    super(message, cause);
  }
}
