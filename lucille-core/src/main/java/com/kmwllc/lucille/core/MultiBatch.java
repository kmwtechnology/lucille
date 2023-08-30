package com.kmwllc.lucille.core;

import java.util.*;

public class MultiBatch implements Batch {

  private final Map<String, Batch> batches;

  private final int capacity;
  private final int timeout;
  private final String indexField;

  /**
   * Represents a reusable "batch" of "batches" that can be filled with Documents.
   * <p>
   * The Documents are placed in separate underlying batches determined by the value of the
   * 'indexField' on each document.
   * <p>
   * A batch is considered to be "expired" if a configured timeout has
   * elapsed since last add or flush. If any of the underlying batches are expired,
   * it will be flushed during the next call to add() or flushIfExpired().
   */
  public MultiBatch(int capacity, int timeout, String indexField) {
    this.batches = new HashMap<>();
    this.capacity = capacity;
    this.timeout = timeout;
    this.indexField = indexField;
  }

  @Override
  public List<Document> add(Document doc) {
    String index = doc.getString(this.indexField);
    Batch batch;

    if (batches.containsKey(index)) {
      batch = batches.get(index);
    } else {
      batch = new SingleBatch(capacity, timeout);
      batches.put(index, batch);
    }

    return batch.add(doc);
  }

  @Override
  public List<Document> flushIfExpired() {
    List<Document> flushedDocs = new LinkedList<>();
    for (Batch batch : batches.values()) {
      flushedDocs.addAll(batch.flushIfExpired());
    }
    return flushedDocs;
  }

  @Override
  public List<Document> flush() {
    List<Document> flushedDocs = new LinkedList<>();
    for (Batch batch : batches.values()) {
      flushedDocs.addAll(batch.flush());
    }
    return flushedDocs;
  }
}
