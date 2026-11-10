package com.kmwllc.lucille.core;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MultiBatch implements Batch {

  private final Map<String, Batch> batches;

  private final int capacity;
  private final long byteCapacity;
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
   * it will be flushed during the next call to add() or flushIfExpired(), regardless of
   * which underlying batch the added document belongs to.
   */
  public MultiBatch(int capacity, long byteCapacity, int timeout, String indexField) {
    this.batches = new HashMap<>();
    this.capacity = capacity;
    this.byteCapacity = byteCapacity;
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
      batch = new SingleBatch(capacity, byteCapacity, timeout);
      batches.put(index, batch);
    }

    // Flush any other sub-batch that has expired, not just the one this document goes into. Under continuous input the
    // Indexer never calls flushIfExpired() (it only does so on an idle poll), so without this a sub-batch for a rarely
    // seen index value could hold its documents for the whole run. With a messenger that commits input offsets on batch
    // completion, such a held document would also hold back the commit frontier of its partition.
    List<Document> flushedDocs = new LinkedList<>();
    for (Batch other : batches.values()) {
      if (other != batch) {
        flushedDocs.addAll(other.flushIfExpired());
      }
    }
    flushedDocs.addAll(batch.add(doc));
    return flushedDocs;
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

  @Override
  public int getCapacity() {
    return capacity;
  }

  @Override
  public void delayExpirationBy(long millis) {
    // Delay the deadline of every per-index sub-batch, so a rarely-touched index's batch is not expired by time the
    // indexer spent sending other indexes' batches.
    for (Batch batch : batches.values()) {
      batch.delayExpirationBy(millis);
    }
  }
}
