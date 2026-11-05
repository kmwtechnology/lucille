package com.kmwllc.lucille.core;

import java.util.Objects;

/**
 * A document that {@link Indexer#sendToIndex(java.util.List)} could not index, and why.
 *
 * @param document The document that failed. Must be the instance passed to sendToIndex, not a copy: two documents with
 *     the same content are different documents, so failures are matched to a batch's documents by identity.
 * @param cause Why the document failed. Use an {@link IndexerRetryableException} if the failure may be transient.
 */
public record IndexerFailure(Document document, Exception cause) {

  public IndexerFailure {
    Objects.requireNonNull(document, "document");
    Objects.requireNonNull(cause, "cause");
  }
}
