package com.kmwllc.lucille.core;

import java.util.List;

/**
 * The outcome of one {@link Indexer#sendToIndex(java.util.List)} call (with retries): exactly one of failures and
 * error is meaningful. error is non-null when the send threw; otherwise failures holds the per-document failures
 * (possibly empty). elapsedNanos is the time the send took, used for the latency metric.
 *
 * <p> A batch can also be <i>not sent</i> ({@link #notSent()}): the messenger reported, before an attempt, that this
 * indexer may no longer own the batch's input (see {@link com.kmwllc.lucille.message.IndexerMessenger#ownsInput}).
 * Neither failures nor error is meaningful then.
 *
 * <p> This is the value a {@link BatchDispatcher} carries from a batch's send to its completion.
 */
final class SendOutcome {

  private final List<IndexerFailure> failures;
  private final Throwable error;
  private final long elapsedNanos;
  private final boolean notSent;

  SendOutcome(List<IndexerFailure> failures, Throwable error, long elapsedNanos) {
    this(failures, error, elapsedNanos, false);
  }

  private SendOutcome(List<IndexerFailure> failures, Throwable error, long elapsedNanos, boolean notSent) {
    this.failures = failures;
    this.error = error;
    this.elapsedNanos = elapsedNanos;
    this.notSent = notSent;
  }

  /** An outcome for a batch that was not sent, or whose retries stopped, because its input may no longer be owned. */
  static SendOutcome inputNotOwned(long elapsedNanos) {
    return new SendOutcome(null, null, elapsedNanos, true);
  }

  List<IndexerFailure> failures() {
    return failures;
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
