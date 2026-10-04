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
    batchCompleter.accept(batchedDocs, outcome);
  }

  @Override
  public void drain() {
  }

  @Override
  public void close() {
  }
}
