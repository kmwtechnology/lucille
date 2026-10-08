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
    SendOutcome outcome = processor.sendWithRetry(batchedDocs);
    // An interrupt (from a forced or timed-out shutdown) can arrive during the send above. The batch has been sent and
    // must still be completed; run completion with the interrupt flag clear, because completeBatch may itself block
    // (e.g. the Hybrid messenger's queue put, which throws if interrupted) and losing a batch's completion would leak
    // its offsets/events. Restore the flag afterward so the run loop still exits.
    boolean interrupted = Thread.interrupted();
    try {
      processor.completeBatch(batchedDocs, outcome);
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public void drain() {
  }

  @Override
  public void close() {
  }
}
