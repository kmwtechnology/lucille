package com.kmwllc.lucille.core;

/**
 * Thrown by {@link Publisher#publish(Document)} and {@link Publisher#flush()} when documents cannot be published: the
 * messaging infrastructure failed, or the thread was interrupted while waiting. It is a framework-level failure, not a
 * problem with a document, so a Connector should let it propagate rather than catch it and continue. The framework's
 * own use of a Publisher (creating it, waiting for a run to complete) reports messaging failures the same way.
 */
public class PublisherException extends Exception {

  public PublisherException(String message) {
    super(message);
  }

  public PublisherException(String message, Throwable cause) {
    super(message, cause);
  }
}
