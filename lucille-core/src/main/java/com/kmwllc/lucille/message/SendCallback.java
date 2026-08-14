package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;

/**
 * Notified when a Document submitted via {@link PublisherMessenger#sendForProcessing(Document, SendCallback)}
 * has been accepted by the destination, or has permanently failed.
 *
 * Exactly one of the two outcomes is delivered per document, unless the send fails synchronously,
 * in which case sendForProcessing throws and the callback is never invoked.
 *
 * Implementations must be thread-safe and must not block. A messenger backed by an asynchronous
 * client invokes this on that client's I/O thread -- for Kafka, the single sender thread that
 * drains the record accumulator -- where blocking would stall every other in-flight send.
 */
@FunctionalInterface
public interface SendCallback {

  /**
   * @param exception null if the Document was accepted by the destination; otherwise the failure
   *                  that prevented it from being accepted, after any retries the client applied.
   */
  void onCompletion(Exception exception);
}
