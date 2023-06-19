package com.kmwllc.lucille.core;

import java.time.Instant;
import java.util.*;

public class MultiBatch implements IBatch {
  private final Map<String, IBatch> batches;

  private final int capacity;
  private final int timeout;
  private final String indexField;

  public MultiBatch(int capacity, int timeout, String indexField){
    this.batches = Collections.synchronizedMap(new HashMap<>());
    this.capacity = capacity;
    this.timeout = timeout;
    this.indexField = indexField;
  }

  @Override
  public List<Document> add(Document doc) {
    String index = doc.getString(this.indexField);
    IBatch batch;
    synchronized (batches) {
      if (batches.containsKey(index)) {
        batch = batches.get(index);
      } else {
        batch = new Batch(capacity, timeout);
        batches.put(index, batch);
      }
    }
    return batch.add(doc);
  }

  @Override
  public List<Document> flushIfExpired() {
    List<Document> flushedDocs = new LinkedList<>();
    for(IBatch batch : batches.values()) {
      flushedDocs.addAll(batch.flushIfExpired());
    }
    return flushedDocs;
  }

  @Override
  public List<Document> flush() {
    List<Document> flushedDocs = new LinkedList<>();
    for(IBatch batch : batches.values()) {
      flushedDocs.addAll(batch.flush());
    }
    return flushedDocs;
  }
}
