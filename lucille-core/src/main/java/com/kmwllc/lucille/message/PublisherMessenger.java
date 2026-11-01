package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;

/**
 * API that a Publisher uses to exchange messages with other components.
 *
 * A Publisher needs to 1) submit Documents for processing and 2) receive Events
 * relating to Documents it has published and their children.
 *
 * <p> Internal to Lucille: not a supported extension point, and it may change in any release. The Runner chooses the
 * messengers for a run; there is no setting for supplying another implementation.
 */
public interface PublisherMessenger {

  /**
   * Sets the Run ID and pipeline that this PublisherMessenger should use.
   * Should be called exactly once.
   *
   * @throws MessengerException if the messaging infrastructure failed, or the thread was interrupted while waiting
   *     (its interrupt flag is restored)
   * @throws IllegalStateException if this PublisherMessenger has already been initialized
   */
  void initialize(String runId, String pipelineName) throws MessengerException;

  /**
   * Returns the ID of the Run in which this PublisherMessenger instance is participating.
   */
  String getRunId();

  /**
   * Submits a Document for processing by a configured pipeline.
   *
   * @throws MessengerException if the messaging infrastructure failed, or the thread was interrupted while waiting
   *     (its interrupt flag is restored)
   */
  void sendForProcessing(Document document) throws MessengerException;

  /**
   * Retrieves and removes an Event waiting to be processed.
   * Should block if no events are available, but should apply a timeout which may
   * be provided when a PublisherMessenger implementation is instantiated.
   * Intended to be called in a polling loop where pollEvent() would periodically timeout
   * so that other conditions can be checked as the loop is waiting for the next event.
   *
   * Events sent via WorkerMessenger.sendEvent() and IndexMessenger.sendEvent()
   * are returned by the current method, PublisherMessenger.pollEvent()
   *
   * @throws MessengerException if the messaging infrastructure failed, or the thread was interrupted while waiting
   *     (its interrupt flag is restored)
   */
  Event pollEvent() throws MessengerException;

  /**
   * Ensures any buffered messages have been delivered. Surfaces any asynchronous send failure.
   * Called at the end of a publishing run, after the last document has been submitted.
   *
   * @throws MessengerException if the messaging infrastructure failed, or the thread was interrupted while waiting
   *     (its interrupt flag is restored)
   */
  default void flush() throws MessengerException {}

  /**
   * Closes any connections opened by this PublisherMessenger.
   *
   * <p> Declares no checked exception: it runs during shutdown, where a caller can only log a failure. An
   * implementation logs a failure to release a resource itself, or lets an unchecked exception propagate.
   */
  void close();
}
