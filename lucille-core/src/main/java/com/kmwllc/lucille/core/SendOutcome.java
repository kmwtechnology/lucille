package com.kmwllc.lucille.core;

import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;

/**
 * The outcome of one {@link Indexer#sendToIndex(java.util.List)} call (with retries): exactly one of failedDocPairs and
 * error is meaningful. error is non-null when the send threw; otherwise failedDocPairs holds the per-document failures
 * (possibly empty). elapsedNanos is the time the send took, used for the latency metric.
 *
 * <p> A batch can also be <i>not sent</i> ({@link #notSent()}): the messenger reported, before an attempt, that this
 * indexer may no longer own the batch's input (see {@link com.kmwllc.lucille.message.IndexerMessenger#ownsInput}).
 * Neither failedDocPairs nor error is meaningful then.
 *
 * <p> This is the value a {@link BatchDispatcher} carries from a batch's send to its completion.
 */
final class SendOutcome {

  private final Set<Pair<Document, Exception>> failedDocPairs;
  private final Throwable error;
  private final long elapsedNanos;
  private final boolean notSent;

  SendOutcome(Set<Pair<Document, Exception>> failedDocPairs, Throwable error, long elapsedNanos) {
    this(failedDocPairs, error, elapsedNanos, false);
  }

  private SendOutcome(Set<Pair<Document, Exception>> failedDocPairs, Throwable error, long elapsedNanos,
      boolean notSent) {
    this.failedDocPairs = failedDocPairs;
    this.error = error;
    this.elapsedNanos = elapsedNanos;
    this.notSent = notSent;
  }

  /** An outcome for a batch that was not sent, or whose retries stopped, because its input may no longer be owned. */
  static SendOutcome inputNotOwned(long elapsedNanos) {
    return new SendOutcome(null, null, elapsedNanos, true);
  }

  Set<Pair<Document, Exception>> failedDocPairs() {
    return failedDocPairs;
  }

  Throwable error() {
    return error;
  }

  long elapsedNanos() {
    return elapsedNanos;
  }

  boolean notSent() {
    return notSent;
  }
}
