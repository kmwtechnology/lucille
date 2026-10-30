package com.kmwllc.lucille.core;

/**
 * Counts how many times a document has been retried, so that a document which keeps failing can be sent to the
 * dead letter queue instead of being redelivered forever. Used by Kafka messengers when worker.maxRetries is set.
 *
 * <p> The implementation is chosen with worker.retryCounter.class and defaults to {@link ZKRetryCounter}.
 * Implementations must have a public constructor that takes the full Lucille Config.
 */
public interface RetryCounter extends AutoCloseable {

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

  /**
   * Releases any resources held by this counter, such as a ZooKeeper client. Called once by the Worker that owns the
   * counter when that Worker stops. Does nothing by default.
   */
  @Override
  default void close() throws Exception {
  }
}
