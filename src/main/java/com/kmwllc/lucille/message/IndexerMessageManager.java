package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.Document;

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
public interface IndexerMessageManager {
  Document pollCompleted() throws Exception;

  void sendEvent(Event event) throws Exception;

  void close() throws Exception;
}
