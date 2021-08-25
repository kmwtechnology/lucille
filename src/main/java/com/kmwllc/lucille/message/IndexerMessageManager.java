package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.Document;

import java.util.List;

/**
 * API that an Indexer uses to exchange messages with other components.
 *
 * An Indexer needs to 1) receive Documents that are ready to be indexed, and 2)
 * send Events indicating the status of the Documents being indexed.
 *
 * An Indexer also needs to communicate with a destination system like Solr or Elasticsearch.
 * A sendToSolr method is currently included on this interface but it may be moved elsewhere eventually.
 */
public interface IndexerMessageManager {
  Document pollCompleted() throws Exception;

  void sendToSolr(List<Document> documents) throws Exception;

  void sendEvent(Event event) throws Exception;

  void close() throws Exception;
}
