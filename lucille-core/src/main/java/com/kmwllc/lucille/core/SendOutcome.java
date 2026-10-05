package com.kmwllc.lucille.core;

import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;

/**
 * The outcome of one {@link Indexer#sendToIndex(java.util.List)} call (with retries): exactly one of failedDocPairs and
 * error is meaningful. error is non-null when the send threw; otherwise failedDocPairs holds the per-document failures
 * (possibly empty). elapsedNanos is the time the send took, used for the latency metric.
 *
 * <p> This is the value a {@link BatchDispatcher} carries from a batch's send to its completion.
 */
final class SendOutcome {

  private final Set<Pair<Document, Exception>> failedDocPairs;
  private final Throwable error;
  private final long elapsedNanos;

  SendOutcome(Set<Pair<Document, Exception>> failedDocPairs, Throwable error, long elapsedNanos) {
    this.failedDocPairs = failedDocPairs;
    this.error = error;
    this.elapsedNanos = elapsedNanos;
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
}
