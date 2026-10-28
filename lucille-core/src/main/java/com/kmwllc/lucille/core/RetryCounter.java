package com.kmwllc.lucille.core;

/**
 * Counts how many times a document has been retried, so that a document which keeps failing can be sent to the
 * dead letter queue instead of being redelivered forever.
 */
public interface RetryCounter extends AutoCloseable {

  boolean add(Document document);

  void remove(Document document);

  /**
   * Releases any resources held by this counter, such as a ZooKeeper client. Called once by the Worker that owns the
   * counter when that Worker stops. Does nothing by default.
   */
  @Override
  default void close() throws Exception {
  }
}
