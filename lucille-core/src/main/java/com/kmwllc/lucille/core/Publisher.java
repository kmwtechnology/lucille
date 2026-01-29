package com.kmwllc.lucille.core;

/**
 * Provides a way to publish documents for processing by the pipeline. Accepts incoming Events relating to
 * published documents and their children. Provides a way to check how many documents are still in a pending
 * state. (A document would be in a pending state if it has been published via the Publisher,
 * or if it is a child document that was created during pipeline execution, but it has not yet
 * reached an end-state of the workflow, e.g. it has not been indexed and has not errored-out.)
 *
 * A new Publisher should be created for each run of a sequence of Connectors. The Publisher is responsible
 * for stamping a designated run_id on each published Document and maintaining accounting details specific to that run.
 *
 * Publisher implementations may support a collapsing mode where consecutive Documents having the same ID are
 * combined into one Document with multi-valued fields.
 *
 */
public interface Publisher {

  /**
   * Submits the given document for processing by any available pipeline worker.
   *
   * Stamps the current Run ID on the document and begins "tracking" events relating the document.
   *
   * IMPORTANT: After calling publish, code should not update the Document or read values from it since at this point
   * it may have been picked up by a worker thread.
   */
  void publish(Document document) throws Exception;

  /**
   * Returns the number of documents published so far.
   *
   */
  long numPublished();

  /**
   * Returns the number of documents for which we are still awaiting a terminal event.
   *
   * This number includes documents published via publish() as well as child documents generated
   * during pipeline execution.
   */
  long numPending();

  /**
   * Returns the number of child documents that the publisher did not publish but was notified about via
   * a CREATE event passed to handleEvent().
   */
  long numCreated();

  /**
   * Returns the number of documents for which the publisher has been notified of successful completion.
   *
   */
  long numSucceeded();

  /**
   * Returns the number of documents for which the publisher has been notified of a failure.
   *
   */
  long numFailed();

  /**
   * Returns the number of documents for which the publisher has received a drop notification.
   *
   */
  long numDropped();

  /**
   * Returns true if there are published documents or generated children than have not yet reached an
   * end state of the workflow (i.e. have not been indexed and have not errored-out). Specifically,
   * this will be true if:
   *
   * 1) a Document was published via publish() but a terminal event has not been received
   * for that document via handleEvent()
   * 2) a CREATE event was received for a child document via handleEvent() but a terminal event for that
   * child document has not been received via handleEvent()
   *
   * Note that a publisher might move from a reconciled state with hasPending()==false
   * back to an unreconciled state with hasPending()==true. This can happen if the
   * publisher receives an Event informing it about a child document that it did not know about previously.
   * After receiving such an Event, the publisher needs to start tracking that child document and will not
   * return to a reconciled state until the document has been indexed.
   *
   * Also note that the publisher may receive terminal events for child documents before receiving the
   * corresponding CREATE events. In this case the terminal events are considered "early" and the CREATE events
   * are considered "late." The publisher can enter a reconciled state before receiving all the late
   * CREATE events that it might be expecting. However, the publisher should still be prepared to receive
   * these late CREATE events and ignore them rather than beginning to track the documents that are already indexed.
   *
   */
  boolean hasPending();

  /**
   * Updates internal accounting based on a received Event: if we learn that a document has
   * been indexed, we can stop tracking it. If we learn that a child document has been
   * newly created, we must start tracking it. If we learn that a child document has been
   * indexed before we began tracking it (i.e. before we received its CREATE event) we need
   * to store its ID separately so that when the CREATE event is eventually received,
   * we'll know NOT to begin tracking it.
   *
   */
  void handleEvent(Event event);

  /**
   * Waits until all published Documents and their children are complete and no more Documents are being published.
   *
   *
   * @param thread a reference to the Connector thread that is generating new Documents;
   *               this thread will be tested for its status via isAlive() to determine when the Connector
   *               has finished and no more Documents will be published
   * @param timeout number of milliseconds to wait, after which the method will return false
   */
  PublisherResult waitForCompletion(ConnectorThread thread, int timeout) throws Exception;

  /**
   * Publish any documents that the Publisher might not have published immediately when publish() was called.
   * This applies mainly to a "collapsing mode" where the Publisher combines consecutive documents having the same
   * ID into a single document. In such a mode, the Publisher would not be able to immediately publish each
   * incoming document because the next might have the same ID and the two should be collapsed. So the Publisher
   * would have to save the previous document and only publish it if the _next_ document has a different ID.
   * At the end of the input, the publisher would be holding on to one previous document and flush() would need
   * to be called to make sure it gets published.
   *
   */
  void flush() throws Exception;

  /**
   * Allows a thread to indicate that it is done calling publish() and that
   * any ThreadLocal resources inside the publisher, maintained for that particular thread,
   * can be released.
   *
   * Should be called by individual publishing threads BEFORE the Runner calls close().
   *
   * Intended for use in concurrent scenario where multiple threads are calling
   * publish() on the same Publisher instance. In general, it should not be
   * necessary to call preClose() if the Connector is single-threaded.
   */
  void preClose() throws Exception;

  /**
   * Closes any connections opened by the publisher.
   *
   */
  void close() throws Exception;

  /**
   * Causes all threads calling publish() to block until resume() is called.
   * If a thread is in the middle of publish() while pause() is called, it will begin
   * blocking the _next_ time publish() is called.
   *
   * @throws IllegalStateException if called when publisher is already paused
   */
  void pause();

  /**
   * Unblocks any threads that were blocked on publish() due to a call to pause().
   * Has no effect on threads that were blocked on publish() for another reason (for example,
   * because the maxPendingDocs threshold was exceeded).
   *
   * @throws IllegalStateException if called when publisher is not paused
   */
  void resume();
}
