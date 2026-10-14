package com.kmwllc.lucille.core;

import java.util.List;

/**
 * Provides the two operations that index a batch: sending it to the destination, then completing it (events and offsets).
 * A {@link BatchDispatcher} is constructed with a BatchProcessor and schedules these two methods, without ever varying
 * what they do. The split exists because the two steps have different thread-safety requirements — {@link #sendWithRetry}
 * may run off the indexer thread, while {@link #completeBatch} must run on it, in dispatch order — which is what lets a
 * concurrent dispatcher overlap sends while keeping completion ordered.
 */
interface BatchProcessor {

  /**
   * Sends one batch to the destination, applying the retry policy, and returns the outcome. May run off the indexer
   * thread (e.g. on a send pool), so it must be safe to call from several threads at once. Never throws: any Throwable
   * is captured in the returned {@link SendOutcome}.
   *
   * @param batchedDocs the documents to send.
   * @return the outcome of the send attempt.
   */
  SendOutcome sendWithRetry(List<Document> batchedDocs);

  /**
   * Completes one sent batch: records metrics, emits FAIL / FINISH events, and (where applicable) commits the input
   * offsets. Must run on the indexer thread, and must be called by the dispatcher in dispatch order.
   *
   * @param batchedDocs the documents that were sent.
   * @param outcome the outcome returned by {@link #sendWithRetry} for this batch.
   */
  void completeBatch(List<Document> batchedDocs, SendOutcome outcome);
}
