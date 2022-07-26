package com.kmwllc.lucille.core;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Represents a reusable "batch" that can be filled with Documents.
 *
 * Once the batch has reached a configured capacity, the next call
 * to add() will flush the batch and return all the Documents it contained.
 * The new Document will not be included in the return value but will
 * be stored as the new first element of the batch.
 *
 * A batch is considered to be "expired" if a configured timeout has
 * elapsed since last add or flush. If a batch is expired,
 * it will be flushed during the next call to add() or flushIfExpired().
 */
public class Batch {

  private final LinkedBlockingQueue<JsonDocument> queue;
  private final int timeout;
  private Instant lastAddOrFlushInstant;

  /**
   * Creates a new batch.
   *
   * @param capacity the number of documents above which the batch will be flushed
   * @param timeout the number of milliseconds (since the previous add or flush) beyond which the batch
   *                will be considered as expired
   */
  public Batch(int capacity, int timeout) {
    this.queue = new LinkedBlockingQueue<>(capacity);
    this.timeout = timeout;
    this.lastAddOrFlushInstant = Instant.now();
  }

  /**
   * Adds a Document to the Batch.
   * If the batch has reached its capacity or if it is expired, it will be flushed
   * and all of its contents will be returned. The newly added document will not
   * be returned but will be stored as the first element of the current batch.
   */
  public List<JsonDocument> add(JsonDocument doc) {
    List<JsonDocument> docs = new ArrayList<>();

    if (isExpired()) {
      queue.drainTo(docs);
    }

    if (!queue.offer(doc)) {
      queue.drainTo(docs);
      queue.offer(doc);
    }

    lastAddOrFlushInstant = Instant.now();
    return docs;
  }

  /**
   * Removes and returns all Documents in the current batch if the batch is expired
   * (i.e. if the configured timeout has been reached since the last add or flush).
   * Returns an empty list otherwise.
   */
  public List<JsonDocument> flushIfExpired() {
    return isExpired() ? flush() : new ArrayList();
  }

  /**
   * Removes and returns all Documents in the current batch,
   * regardless of whether the batch is expired.
   */
  public List<JsonDocument> flush() {
    List<JsonDocument> docs = new ArrayList<>();
    queue.drainTo(docs);
    lastAddOrFlushInstant = Instant.now();
    return docs;
  }

  /**
   * Indicates whether the configured timeout has elapsed since the most
   * recent of the following events: add(), flush(), flushIfExpired() with an expiration detected, new Batch().
   */
  private boolean isExpired() {
    return ChronoUnit.MILLIS.between(lastAddOrFlushInstant, Instant.now()) > timeout;
  }

}
