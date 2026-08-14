package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;

/**
 * API that a Publisher uses to exchange messages with other components.
 *
 * A Publisher needs to 1) submit Documents for processing and 2) receive Events
 * relating to Documents it has published and their children.
 *
 */
public interface PublisherMessenger {

  /**
   * Sets the Run ID and pipeline that this PublisherMessenger should use.
   * Should be called exactly once.
   */
  void initialize(String runId, String pipelineName) throws Exception;

  /**
   * Returns the ID of the Run in which this PublisherMessenger instance is participating.
   */
  String getRunId();

  /**
   * Submits a Document for processing by a configured pipeline, blocking until the destination has
   * accepted it.
   */
  void sendForProcessing(Document document) throws Exception;

  /**
   * Submits a Document for processing by a configured pipeline, invoking the given callback when
   * the destination has accepted the Document or permanently failed to.
   *
   * Implementations backed by an asynchronous client should return as soon as the Document has been
   * handed to that client, so that the calling thread does not wait out a round trip per document.
   * A failure detected before handing the Document off is thrown from this method rather than
   * reported to the callback; once handed off, all failures arrive via the callback.
   *
   * The default implementation is the synchronous send followed by an inline callback, which is
   * correct for any messenger that completes the send before returning.
   */
  default void sendForProcessing(Document document, SendCallback callback) throws Exception {
    sendForProcessing(document);
    callback.onCompletion(null);
  }

  /**
   * Retrieves and removes an Event waiting to be processed.
   * Should block if no events are available, but should apply a timeout which may
   * be provided when a PublisherMessenger implementation is instantiated.
   * Intended to be called in a polling loop where pollEvent() would periodically timeout
   * so that other conditions can be checked as the loop is waiting for the next event.
   *
   * Events sent via WorkerMessenger.sendEvent() and IndexMessenger.sendEvent()
   * are returned by the current method, PublisherMessenger.pollEvent()
   */
  Event pollEvent() throws Exception;

  /**
   * Closes any connections opened by this PublisherMessenger.
   */
  void close();
}
