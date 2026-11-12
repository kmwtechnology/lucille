package com.kmwllc.lucille.core;

import java.util.List;

/**
 * One call an {@link Indexer} is about to make to {@link Indexer#sendToIndex(PendingSend)}: the documents to send now,
 * and where this call falls among the attempts made for their batch.
 *
 * <p> The Indexer creates a new PendingSend for every attempt. On the first attempt it holds the whole batch; on a retry
 * it holds only the documents that are still pending, which are the ones the previous attempt reported with a retryable
 * failure or as {@link IndexerFailure#notSent(Document) not sent}. Documents are the same instances on every attempt.
 *
 * <p> Instances are immutable. Only the Indexer creates them, other than through {@link #of(List)}, so that members can
 * be added without changing any Indexer implementation.
 */
public final class PendingSend {

  private final List<Document> documents;
  private final int attempt;
  private final boolean isFinal;

  PendingSend(List<Document> documents, int attempt, boolean isFinal) {
    this.documents = List.copyOf(documents);
    this.attempt = attempt;
    this.isFinal = isFinal;
  }

  /**
   * A send that is made once, with no retry: {@link #attempt()} is 1 and {@link #isFinal()} is true. For tests and
   * other direct calls to sendToIndex.
   *
   * @param documents The documents to send.
   * @return A PendingSend holding the given documents.
   */
  public static PendingSend of(List<Document> documents) {
    return new PendingSend(documents, 1, true);
  }

  /**
   * The documents to send now, in the order they were added to their batch: the whole batch on the first attempt, then
   * only those still pending.
   *
   * @return An unmodifiable list of the documents to send.
   */
  public List<Document> documents() {
    return documents;
  }

  /**
   * This attempt's number, starting at 1.
   *
   * @return The number of this attempt.
   */
  public int attempt() {
    return attempt;
  }

  /**
   * Whether no retry can follow this attempt, whatever its outcome. Always true when retries are not configured.
   *
   * @return true if this is the last attempt that will be made for these documents.
   */
  public boolean isFinal() {
    return isFinal;
  }
}
