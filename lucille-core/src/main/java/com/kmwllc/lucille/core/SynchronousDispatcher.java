package com.kmwllc.lucille.core;

import com.kmwllc.lucille.core.Indexer.SendOutcome;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * The default {@link BatchDispatcher}: sends each batch on the calling (indexer) thread and completes it before
 * returning. This is Lucille's historical behavior, used whenever indexer.maxConcurrentBatches is 1. It holds no
 * resources, so {@link #drain()} and {@link #close()} do nothing.
 */
class SynchronousDispatcher implements BatchDispatcher {

  private final Function<List<Document>, SendOutcome> batchSender;
  private final BiConsumer<List<Document>, SendOutcome> batchCompleter;

  SynchronousDispatcher(Function<List<Document>, SendOutcome> batchSender,
      BiConsumer<List<Document>, SendOutcome> batchCompleter) {
    this.batchSender = batchSender;
    this.batchCompleter = batchCompleter;
  }

  @Override
  public void dispatch(List<Document> batchedDocs) {
    SendOutcome outcome = batchSender.apply(batchedDocs);
    // An interrupt (from a forced or timed-out shutdown) can arrive during the send above. The batch has been sent and
    // must still be completed; run completion with the interrupt flag clear, because batchComplete may itself block
    // (e.g. the Hybrid messenger's queue put, which throws if interrupted) and losing a batch's completion would leak
    // its offsets/events. Restore the flag afterward so the run loop still exits.
    boolean interrupted = Thread.interrupted();
    try {
      batchCompleter.accept(batchedDocs, outcome);
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
