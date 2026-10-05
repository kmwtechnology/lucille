package com.kmwllc.lucille.core;

import com.kmwllc.lucille.core.Indexer.SendOutcome;
import java.util.List;

/**
 * The two steps of indexing a batch, supplied to a {@link BatchDispatcher} by the {@link Indexer} that owns it (the
 * Indexer implements this interface). The dispatcher schedules these — it never varies what they do — and they carry
 * different threading contracts, which is the whole reason a concurrent dispatcher exists: {@link #sendWithRetry} may
 * run off the indexer thread (on a pool), while {@link #completeBatch} must run on the indexer thread, in dispatch
 * order.
 */
interface BatchProcessor {

  /**
   * Sends one batch to the destination, applying the retry policy, and returns the outcome. Never throws: any Throwable
   * is captured in the returned {@link SendOutcome}. May run off the indexer thread.
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
