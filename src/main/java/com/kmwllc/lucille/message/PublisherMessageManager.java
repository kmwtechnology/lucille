package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;

/**
 * API that a Publisher uses to exchange messages with other components.
 *
 * A Publisher needs to 1) submit Documents for processing and 2) receive Events
 * relating to Documents it has published and their children.
 *
 * Implementations of Publisher are expected to accept a run ID at construction time and use the run ID
 * to determine where to send and receive events relating to that particular run.
 */
public interface PublisherMessageManager {

  void sendForProcessing(Document document) throws Exception;

  Event pollEvent() throws Exception;

  boolean hasEvents() throws Exception;

  void close();
}
