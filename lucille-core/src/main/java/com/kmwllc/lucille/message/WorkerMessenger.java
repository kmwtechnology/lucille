package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;

/**
 * API that a Worker uses to exchange messages with other components.
 *
 * A Worker needs a way to 1) receive Documents to process, 2) send processed Documents to
 * a destination where they can wait for indexing, and 3) send Events relating to
 * Documents being processed (e.g. if a child Document is created, if a Document can't be processed, etc.)
 *
 */
public interface WorkerMessenger {

  /**
   * Retrieve a Document that has been published (via PublisherMessenger.sendForProcessing())
   * and is now ready to be processed by a given pipeline; block if no such Document is available,
   * but apply a timeout of several milliseconds to several seconds so that this method can be called
   * from within a polling loop that periodically checks other conditions even when no Documents are available.
   *
   * A Documents retrieved by pollDocToProcess() may not be "removed" or made invisible to subsequent calls
   * until commitPendingDocOffsets() has been called.
   */
  Document pollDocToProcess() throws Exception;

  /**
   * Indicates that Documents previously retrieved by pollDocToProcess() have been processed and should
   * not be returned by subsequent calls.
   */
  void commitPendingDocOffsets() throws Exception;

  /**
   * Submit a given Document so that it can be received by an Indexer component that
   * would call IndexerMessenger.pollCompleted()
   */
  void sendForIndexing(Document document) throws Exception;

  /**
   * Submit a given Document to a "Dead Letter Queue" for Documents that cannot be processed.
   */
  void sendFailed(Document document) throws Exception;

  /**
   * Create an Event from the given parameters and make it available to the Publisher or
   * any other component that is listening for Document-related Events.
   */
  void sendEvent(Document document, String message, Event.Type type) throws Exception;

  /**
   * Make the designated Event available to the Publisher or any other component that is listening for
   * Document-related Events.
   */
  void sendEvent(Event event) throws Exception;

  /**
   * Close any connections opened by this WorkerMessenger.
   */
  void close() throws Exception;
}
