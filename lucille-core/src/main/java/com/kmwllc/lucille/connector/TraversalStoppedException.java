package com.kmwllc.lucille.connector;

/**
 * Thrown to stop a traversal because continuing could only skip or fail every remaining file the same way, for
 * example when the messenger can no longer send documents. Escapes the per-file error handling and fails the
 * traversal.
 */
public class TraversalStoppedException extends RuntimeException {

  public TraversalStoppedException(String message, Throwable cause) {
    super(message, cause);
  }
}
