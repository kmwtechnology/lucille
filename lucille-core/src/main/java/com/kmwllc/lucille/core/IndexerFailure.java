package com.kmwllc.lucille.core;

import java.util.Objects;

/**
 * A document that {@link Indexer#sendToIndex(PendingSend)} did not index, and why.
 *
 * @param document The document that failed. Must be an instance from the send's documents, not a copy: two documents
 *     with the same content are different documents, so failures are matched to a send's documents by identity. A
 *     failure that can't be matched fails the rest of the batch.
 * @param cause Why the document failed. Use an {@link IndexerRetryableException} if the failure may be transient.
 */
public record IndexerFailure(Document document, Exception cause) {

  public IndexerFailure {
    Objects.requireNonNull(document, "document");
    Objects.requireNonNull(cause, "cause");
  }

  /**
   * A failure for a document that the Indexer chose not to send in this call, because sending it had to wait for an
   * earlier document in the same send that failed. The document stays pending: it is included in the next attempt if
   * one is made. A not-sent document never causes a retry by itself, and if no further attempt is made it is reported
   * as failed.
   *
   * @param document The document that was not sent. Must be an instance from the send's documents.
   * @return A failure marking the document as not sent.
   */
  public static IndexerFailure notSent(Document document) {
    return new IndexerFailure(document, new NotSentException());
  }

  /**
   * Whether this failure was created by {@link #notSent(Document)}.
   *
   * @return true if the document was held back and not sent.
   */
  public boolean isNotSent() {
    return cause instanceof NotSentException;
  }

  // The cause of a not-sent failure. It only marks the failure, so it carries no stack trace.
  private static final class NotSentException extends IndexerException {

    NotSentException() {
      super("The document was not sent, because an earlier document in its batch failed.");
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
    }
  }
}
