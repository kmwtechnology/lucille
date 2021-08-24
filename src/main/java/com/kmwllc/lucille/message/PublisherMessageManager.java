package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;

public interface PublisherMessageManager {

  void sendForProcessing(Document document) throws Exception;

  Event pollEvent() throws Exception;

  boolean hasEvents(String runId) throws Exception;

  void close();
}
