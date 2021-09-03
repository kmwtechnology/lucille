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
  private Instant mostRecentAdded;

  public Batch(int size, int timeout) {
    this.queue = new LinkedBlockingQueue<>(size);
    this.size = size;
    this.timeout = timeout;
    this.mostRecentAdded = Instant.now();
  }

  public List<Document> add(Document doc) {
    List<Document> docs = new ArrayList<>();

    long elapsed = ChronoUnit.MILLIS.between(mostRecentAdded, Instant.now());
    if (elapsed > timeout) {
      queue.drainTo(docs);
      mostRecentAdded = Instant.now();
    }

    if (doc == null) {
      return docs;
    }

    if (!queue.offer(doc)) {
      queue.drainTo(docs);
      queue.offer(doc);
    }

    mostRecentAdded = Instant.now();

    return docs;
  }

  public List<Document> flush() {
    List<Document> docs = new ArrayList<>();
    queue.drainTo(docs);
    mostRecentAdded = Instant.now();
    return docs;
  }


}
