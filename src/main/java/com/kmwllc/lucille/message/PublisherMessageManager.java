package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.Event;

/**
 * API that a Publisher uses to exchange messages with other components.
 *
 * A Publisher needs to 1) submit Documents for processing and 2) receive Events
 * relating to Documents it has published and their children.
 *
 */
public interface PublisherMessageManager {

  /**
   * Sets the Run ID and pipeline that this PublisherMessageManager should use.
   * Should be called exactly once.
   */
  void initialize(String runId, String pipelineName) throws Exception;

  /**
   * Returns the ID of the Run in which this PublisherMessageManager instance is participating.
   */
  String getRunId();

  /**
   * Submits a Document for processing by a configured pipeline.
   */
  void sendForProcessing(JsonDocument document) throws Exception;

  /**
   * Retrieves and removes an Event waiting to be processed.
   * Should block if no events are available, but should apply a timeout which may
   * be provided when a PublisherMessageManager implementation is instantiated.
   * Intended to be called in a polling loop where pollEvent() would periodically timeout
   * so that other conditions can be checked as the loop is waiting for the next event.
   *
   * Events sent via WorkerMessageManager.sendEvent() and IndexMessageManager.sendEvent()
   * are returned by the current method, PublisherMessageManager.pollEvent()
   */
  Event pollEvent() throws Exception;

  /**
   * Closes any connections opened by this PublisherMessageManager.
   */
  void close();
}
