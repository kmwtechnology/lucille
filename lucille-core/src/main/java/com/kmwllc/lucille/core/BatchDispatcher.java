package com.kmwllc.lucille.core;

import java.util.List;

/**
 * Strategy for scheduling the two steps of indexing a batch.
 *
 * <p> Indexing a batch of documents involves two steps:
 * <ul>
 *   <li><b>Sending</b> the documents to the destination. The specific Indexer subclass implements a single attempt,
 *   {@link Indexer#sendToIndex(List)}; the Indexer base class wraps that attempt in the configured retry policy.</li>
 *   <li><b>Completing</b> the batch — a post-processing step owned by the Indexer base class — which does the
 *   accounting for the batch: recording metrics, emitting FAIL / FINISH events to the event queue or topic, and
 *   committing the input offsets where applicable.</li>
 * </ul>
 *
 * <p> A straightforward Indexer just runs these in sequence: send a batch, complete it, then move on to the next. A
 * dispatcher decouples the two steps so that a concurrent implementation can send several batches (on a pool) before
 * the first one is completed, while the completions still happen one at a time on the indexer thread.
 *
 * <p> The Indexer gives the dispatcher two callbacks when it constructs it, and the dispatcher decides, for each batch
 * it accepts, when to pass the batch through them:
 * <ul>
 *   <li>a <b>batch sender</b> that sends one batch (with retries) and returns its {@link Indexer.SendOutcome}; it never
 *   throws, capturing any Throwable in the outcome instead. It may run off the indexer thread (on a pool).</li>
 *   <li>a <b>batch completer</b> that completes one sent batch. It touches messenger state and must always run on the
 *   indexer thread.</li>
 * </ul>
 *
 * <p> The dispatcher owns only this <i>scheduling</i> — which thread a send runs on, and when each batch's completer is
 * invoked. It never inspects or alters what a send or a completion does. Implementations must invoke the completer for
 * batches in the order the batches were dispatched (even if their sends finish out of order), so that a messenger which
 * commits its input on completion never commits past an unfinished batch.
 */
interface BatchDispatcher extends AutoCloseable {

  /**
   * Hands a flushed, non-empty batch to the dispatcher. May block to respect scheduling limits. The batch's completer
   * is invoked, in dispatch order relative to other batches, once the batch has been sent.
   *
   * @param batchedDocs the documents to send; must not be empty.
   */
  void dispatch(List<Document> batchedDocs);

  /**
   * Completes every batch that has been dispatched but not yet completed. Called when the indexer stops, so that no
   * batch is left unaccounted for.
   */
  void drain();

  /** Releases any resources held by the dispatcher (such as a thread pool). */
  @Override
  void close();
}
