package com.kmwllc.lucille.core;

import com.kmwllc.lucille.core.Indexer.SendOutcome;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Sends each batch on the indexer thread and completes it before returning, so at most one batch is in flight. Used when
 * indexer.maxConcurrentBatches is 1 (the default). With nothing ever left in flight, completeFinished, completeAll, and
 * close have nothing to do.
 */
class SynchronousBatchSender implements BatchSender {

  private final Function<List<Document>, SendOutcome> send;
  private final BiConsumer<List<Document>, SendOutcome> complete;

  /**
   * @param send sends a batch; must not throw.
   * @param complete completes a sent batch.
   */
  SynchronousBatchSender(Function<List<Document>, SendOutcome> send, BiConsumer<List<Document>, SendOutcome> complete) {
    this.send = send;
    this.complete = complete;
  }

  @Override
  public void send(List<Document> batch) {
    complete.accept(batch, send.apply(batch));
  }

  @Override
  public void completeFinished() {
  }

  @Override
  public void completeAll() {
  }

  @Override
  public void close() {
  }
}
