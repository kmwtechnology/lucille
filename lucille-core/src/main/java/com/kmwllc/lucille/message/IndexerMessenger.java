package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;

import java.util.List;

/**
 * API that an Indexer uses to exchange messages with other Lucille components.
 *
 * An Indexer needs to 1) receive Documents that are ready to be indexed, and 2)
 * send Events indicating the status of the Documents being indexed.
 *
 * An Indexer also needs to communicate with a destination system like Solr or Elasticsearch but
 * that communication is not included in this interface.
 */
public interface IndexerMessenger {

  /**
   * Retrieve and remove a Document that has been processed by a pipeline and is now ready to be
   * indexed; block if no such Document is available, but apply a timeout of several milliseconds
   * to several seconds so that this method can be called from within a polling loop that
   * periodically checks other conditions even when no Documents are available.
   */
  Document pollDocToIndex() throws Exception;

  /**
   * Make the designated Event available to the Publisher or any other component that is listening for
   * Document-related Events.
   */
  void sendEvent(Event event) throws Exception;

  /**
   * Create an Event from the given parameters and make it available to the Publisher or
   * any other component that is listening for Document-related Events.
   */
  void sendEvent(Document document, String message, Event.Type type) throws Exception;

  /**
   * Sends Events of the given type and message for a batch of Documents. Implementations may
   * send the events asynchronously and flush at the end, providing better throughput than
   * calling sendEvent() individually for each document.
   */
  void sendEvents(List<Document> documents, String message, Event.Type type) throws Exception;

  /**
   * Close any connections opened by this IndexerMessenger.
   */
  void close() throws Exception;

  /**
   * Provides a way to communicate to other components that a batch of documents has been completed.
   */
  void batchComplete(List<Document> batch) throws Exception;

  /**
   * Registers a hook the messenger runs, on the indexer thread, when it learns a partition is about to be revoked in a
   * consumer-group rebalance — before the partition is handed off. The Indexer registers an action here that flushes
   * its open batch and drains in-flight batches, so their offsets are committed before the handoff and a <i>clean</i>
   * rebalance does not re-deliver them (re-delivery is then limited to crashes, where the hook never runs). The hook
   * owns the batch and dispatcher, which are confined to the indexer thread; a messenger must therefore only invoke it
   * from that thread, and only from within {@link #pollDocToIndex()} or {@link #close()} — never while the Indexer is
   * dispatching or draining a batch. (A Kafka messenger's rebalance callback runs inside the consumer's poll(), which
   * it calls only from those two methods.) The default does nothing — only a messenger backed by a rebalancing consumer
   * needs it.
   */
  default void setPreRevocationHook(Runnable hook) {
  }

  /**
   * Whether this messenger commits (or acknowledges) its input on {@link #batchComplete(List)} rather than at poll —
   * the property that makes it safe to run an Indexer with indexer.maxConcurrentBatches greater than 1. With
   * concurrency, documents are polled while earlier batches are still being sent, so a messenger that commits its input
   * at poll time could lose in-flight documents on a crash and must return false. This is a distinct check from
   * {@code Indexer.supportsConcurrentSends()}, which instead asserts the destination <i>client</i> is thread-safe; both
   * must hold for concurrency to be enabled. The default is false, so an unknown messenger disables concurrency rather
   * than risking data loss; messengers that commit on completion (or do not commit input at all) override this to true.
   */
  default boolean commitsOnBatchCompletion() {
    return false;
  }
}
