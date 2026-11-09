package com.kmwllc.lucille.core;

import java.util.List;

/**
 * The outcome of one {@link Indexer#sendToIndex(java.util.List)} call (with retries): exactly one of failures and
 * error is meaningful. error is non-null when the send threw; otherwise failures holds the per-document failures
 * (possibly empty). elapsedNanos is the time the send took, used for the latency metric.
 *
 * <p> This is the value a {@link BatchDispatcher} carries from a batch's send to its completion.
 */
final class SendOutcome {

  private final List<IndexerFailure> failures;
  private final Throwable error;
  private final long elapsedNanos;

  SendOutcome(List<IndexerFailure> failures, Throwable error, long elapsedNanos) {
    this.failures = failures;
    this.error = error;
    this.elapsedNanos = elapsedNanos;
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
}
