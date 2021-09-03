package com.kmwllc.lucille.core;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class Batch {

  private final LinkedBlockingQueue<Document> queue;
  private final int size;
  private final int timeout;
  private  Instant batchStart;

  public Batch(int size, int timeout) {
    this.queue = new LinkedBlockingQueue<>(size);
    this.size = size;
    this.timeout = timeout;
    this.batchStart = Instant.now();
  }

  public List<Document> add(Document doc) {
    List<Document> docs = new ArrayList<>();

    long elapsed = ChronoUnit.MILLIS.between(batchStart, Instant.now());
    if (elapsed > timeout) {
      queue.drainTo(docs);
      batchStart = Instant.now();
    }

    if (doc == null) {
      return docs;
    }

    if (!queue.offer(doc)) {
      queue.drainTo(docs);
      queue.offer(doc);
      batchStart = Instant.now();
    }

    return docs;
  }

  public List<Document> flush() {
    List<Document> docs = new ArrayList<>();
    queue.drainTo(docs);
    batchStart = Instant.now();
    return docs;
  }


}
