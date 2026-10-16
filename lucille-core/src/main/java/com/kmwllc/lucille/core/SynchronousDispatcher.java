package com.kmwllc.lucille.core;

import java.util.List;

/**
 * The default {@link BatchDispatcher}: sends each batch on the calling (indexer) thread and completes it before
 * returning. This is Lucille's historical behavior, used whenever indexer.maxConcurrentBatches is 1. It holds no
 * resources, so {@link #drain()} and {@link #close()} do nothing.
 */
class SynchronousDispatcher implements BatchDispatcher {

  private final BatchProcessor processor;

  SynchronousDispatcher(BatchProcessor processor) {
    this.processor = processor;
  }

  @Override
  public void dispatch(List<Document> batchedDocs) {
    processor.completeBatch(batchedDocs, processor.sendWithRetry(batchedDocs));
  }

  @Override
  public void drain() {
  }

  @Override
  public void close() {
  }
}
