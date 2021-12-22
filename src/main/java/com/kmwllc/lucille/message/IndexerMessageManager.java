package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.Document;

/**
 * API that an Indexer uses to exchange messages with other Lucille components.
 *
 * An Indexer needs to 1) receive Documents that are ready to be indexed, and 2)
 * send Events indicating the status of the Documents being indexed.
 *
 * An Indexer also needs to communicate with a destination system like Solr or Elasticsearch but
 * that communication is not included in this interface.
 */
public interface IndexerMessageManager {

  /**
   * Retrieve and remove a Document that has been processed by a pipeline and is now ready to be
   * indexed; block if no such Document is available, but apply a timeout of several milliseconds
   * to several seconds so that this method can be called from within a polling loop that
   * periodically checks other conditions even when no Documents are available.
   */
  Document pollCompleted() throws Exception;

  /**
   * Make the designated Event available to the Publisher or any other component that is listening for
   * Document-related Events.
   */
  void sendEvent(Event event) throws Exception;

  /**
   * Close any connections opened by this IndexerMessageManager.
   */
  void close() throws Exception;
}
