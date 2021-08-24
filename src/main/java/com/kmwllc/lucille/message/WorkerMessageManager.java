package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.Document;

public interface WorkerMessageManager {
  Document pollDocToProcess() throws Exception;

  void sendCompleted(Document document) throws Exception;

  void sendEvent(Event event) throws Exception;

  void close() throws Exception;
}
