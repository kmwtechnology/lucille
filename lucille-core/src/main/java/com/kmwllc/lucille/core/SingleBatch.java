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
  private final int capacity;
  private final long byteCapacity;
  private long byteAccumulator = 0;

  /**
   * Creates a new batch.
   *
   * @param capacity the number of documents above which the batch will be flushed
   * @param byteCapacity the accumulated document byte size above which the batch will be flushed, or
   * {@link Indexer#NO_BATCH_SIZE_BYTES} to disable byte-based flushing
   * @param timeout the number of milliseconds (since the previous add or flush) beyond which the batch
   *                will be considered as expired
   */
  public SingleBatch(int capacity, long byteCapacity, int timeout) {
    this.capacity = capacity;
    this.byteCapacity = byteCapacity;
    this.queue = new LinkedBlockingQueue<>(capacity);
    this.timeout = timeout;
    this.lastAddOrFlushInstant = Instant.now();
  }


  @Override
  public List<Document> add(Document doc) {
    List<Document> docs = new ArrayList<>();
    // can just leave this at 0 if byte cap isn't set so that we don't have to call getByteSize()
    long docByteSize = byteCapacity != Indexer.NO_BATCH_SIZE_BYTES ? doc.getByteSize() : 0;
    byteAccumulator = byteAccumulator + docByteSize;

    if (isExpired()) {
      queue.drainTo(docs);
      byteAccumulator = docByteSize;
    }

    if (byteAccumulator > byteCapacity || !queue.offer(doc)) {
      queue.drainTo(docs);
      queue.offer(doc);
      byteAccumulator = docByteSize;
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
    byteAccumulator = 0;
    return docs;
  }

  @Override
  public int getCapacity() {
    return capacity;
  }

  /**
   * Indicates whether the configured timeout has elapsed since the most
   * recent of the following events: add(), flush(), flushIfExpired() with an expiration detected, new Batch().
   */
  private boolean isExpired() {
    return ChronoUnit.MILLIS.between(lastAddOrFlushInstant, Instant.now()) > timeout;
  }

}
