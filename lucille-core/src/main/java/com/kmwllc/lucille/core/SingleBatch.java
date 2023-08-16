package com.kmwllc.lucille.core;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class SingleBatch implements Batch {

  private final LinkedBlockingQueue<Document> queue;
  private final int timeout;
  private Instant lastAddOrFlushInstant;

  /**
   * Creates a new batch.
   *
   * @param capacity the number of documents above which the batch will be flushed
   * @param timeout the number of milliseconds (since the previous add or flush) beyond which the
   *     batch will be considered as expired
   */
  public SingleBatch(int capacity, int timeout) {
    this.queue = new LinkedBlockingQueue<>(capacity);
    this.timeout = timeout;
    this.lastAddOrFlushInstant = Instant.now();
  }

  @Override
  public List<Document> add(Document doc) {
    List<Document> docs = new ArrayList<>();

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

  @Override
  public List<Document> flushIfExpired() {
    return isExpired() ? flush() : new ArrayList();
  }

  @Override
  public List<Document> flush() {
    List<Document> docs = new ArrayList<>();
    queue.drainTo(docs);
    lastAddOrFlushInstant = Instant.now();
    return docs;
  }

  /**
   * Indicates whether the configured timeout has elapsed since the most recent of the following
   * events: add(), flush(), flushIfExpired() with an expiration detected, new Batch().
   */
  private boolean isExpired() {
    return ChronoUnit.MILLIS.between(lastAddOrFlushInstant, Instant.now()) > timeout;
  }
}
