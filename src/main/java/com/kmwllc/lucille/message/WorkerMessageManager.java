package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.Document;

/**
 * API that a Worker uses to exchange messages with other components.
 *
 * A Worker needs a way to 1) receive Documents to process, 2) send processed Documents to
 * a destination, and 3) send Events relating to Documents being processed (e.g. if a child Document is
 * created, if a Document can't be processed, etc.)
 *
 */
public interface WorkerMessageManager {
  Document pollDocToProcess() throws Exception;

  void sendCompleted(Document document) throws Exception;

  void sendEvent(Event event) throws Exception;

  void close() throws Exception;
}
