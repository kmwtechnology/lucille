package com.kmwllc.lucille.callback;

import com.kmwllc.lucille.core.Document;

public interface WorkerMessageManager {
  Document pollDocToProcess() throws Exception;

  void sendCompleted(Document document) throws Exception;

  void sendEvent(Event event) throws Exception;

  void close() throws Exception;
}
