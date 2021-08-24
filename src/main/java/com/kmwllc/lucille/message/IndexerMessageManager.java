package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.Document;

import java.util.List;

public interface IndexerMessageManager {
  Document pollCompleted() throws Exception;

  void sendToSolr(List<Document> documents) throws Exception;

  void sendEvent(Event event) throws Exception;

  void close() throws Exception;
}
