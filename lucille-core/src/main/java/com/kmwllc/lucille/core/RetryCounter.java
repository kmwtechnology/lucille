package com.kmwllc.lucille.core;

/**
 * Tracks how many times processing has been attempted for each document, across all Workers, so that a document
 * that keeps failing can be sent to the dead letter queue. Used when worker.maxRetries is set.
 *
 * <p> The implementation is chosen with worker.retryCounter.class and defaults to {@link ZKRetryCounter}.
 * Implementations must have a public constructor that takes the full Lucille Config.
 */
public interface RetryCounter {

  /**
   * Records an attempt to process the given document.
   *
   * @param document The document about to be processed.
   * @return true if the document has exceeded the maximum number of retries and should not be processed again.
   */
  boolean add(Document document);

  /**
   * Removes the retry count for the given document once it has been handled.
   *
   * @param document The document whose count should be removed.
   */
  void remove(Document document);
}
