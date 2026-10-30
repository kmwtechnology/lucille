package com.kmwllc.lucille.core;

/**
 * Accepts the documents a {@link Connector} produces and makes them available for processing by the pipeline. This is
 * the interface a Connector is given in {@link Connector#execute(Publisher)}; tracking the run's documents through
 * the pipeline and deciding when the run is complete are the framework's job, not the Connector's.
 *
 * <p> A new Publisher is created for each run of a Connector. It stamps the run's ID on each published Document. A
 * Connector, or another component watching the run, can also read the run's progress ({@link #numReceived()},
 * {@link #numPublished()}, {@link #numPending()}, {@link #hasPending()}, {@link #numCreated()},
 * {@link #numSucceeded()}, {@link #numFailed()}, {@link #numDropped()}), see whether the Connector is still publishing
 * ({@link #isPublishing()}, {@link #hasFinishedPublishing()}), and control the flow of documents with
 * {@link #pause()} and {@link #resume()}.
 *
 * <p> Thread safety: the progress and state methods, {@link #pause()} and {@link #resume()} may be called from any
 * thread. {@link #publish(Document)} and {@link #flush()} may be called from several threads at once, except when the
 * Publisher is in collapsing mode (the Connector's {@link Connector#requiresCollapsingPublisher()} returns true). In
 * collapsing mode, consecutive Documents with the same ID are combined into one, which only makes sense for Documents
 * published one after another from a single thread; overlapping calls fail with an IllegalStateException.
 */
public interface Publisher {

  /**
   * Submits the given document for processing by any available pipeline worker.
   *
   * <p> Stamps the current Run ID on the document and begins tracking events relating to it.
   *
   * <p> IMPORTANT: After calling publish, code should not update the Document or read values from it, since at this
   * point it may have been picked up by a worker thread.
   *
   * <p> May block: when the publisher has been paused, or when the number of documents still in the pipeline has
   * reached {@code publisher.maxPendingDocs}.
   *
   * @throws PublisherException if the document cannot be published: the messaging infrastructure failed, or the
   *     thread was interrupted while waiting to publish (its interrupt flag is restored). This is a framework-level
   *     failure: Connectors must not catch it and continue, since doing so may leave the run in an inconsistent
   *     state. A Connector that needs to handle per-document problems, such as failing to construct a Document,
   *     should do so before calling publish.
   * @throws IllegalStateException if the run this Publisher belongs to has ended, or if overlapping calls are made
   *     in collapsing mode
   */
  void publish(Document document) throws PublisherException;

  /**
   * Makes sure that every document passed to {@link #publish(Document)} so far has been sent for processing: in
   * collapsing mode, publishes the document the Publisher is holding back in case the next one has the same ID, and
   * then waits for the messaging infrastructure to deliver what it has buffered (with Kafka, until the producer has
   * sent every record).
   *
   * <p> The framework flushes once after {@link Connector#execute(Publisher)} returns, so a Connector only needs to
   * call this when it must be sure documents have been delivered while it is still running, for example before
   * recording a checkpoint. Calling it more than once is harmless. In collapsing mode, the next document published
   * after a flush is not combined with the ones before it, even if it has the same ID.
   *
   * @throws PublisherException if a document could not be delivered: the messaging infrastructure failed, now or for
   *     an earlier asynchronous send. Like a failure in publish, this is a framework-level failure that a Connector
   *     must not catch and continue from.
   * @throws IllegalStateException if the run this Publisher belongs to has ended, or if the call overlaps a call to
   *     publish or flush in collapsing mode
   */
  void flush() throws PublisherException;

  /**
   * Returns the number of documents published so far.
   *
   */
  long numPublished();

  /**
   * Returns the number of times publish() was called and completed without error.
   * This may be higher than numPublished() if the Publisher is in "collapsing mode" where consecutive Documents
   * with the same ID are collapsed into a single Document. numPublished() would count 1 for each output of the collapsing process
   * while numReceived() would count 1 for each input.
   */
  long numReceived();

  /**
   * Returns the number of distinct document IDs for which we are still awaiting a terminal event.
   *
   * This count includes documents published via publish() as well as child documents generated
   * during pipeline execution. If the same document ID is published multiple times (e.g. in
   * streaming mode), it is counted once here, not once per occurrence.
   */
  long numPending();

  /**
   * Returns the limit on the number of pending documents ({@code publisher.maxPendingDocs}) at which
   * {@link #publish(Document)} blocks until documents complete, or null if there is no limit.
   */
  Integer getMaxPendingDocs();

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
   * Returns the number of child documents that the publisher did not publish but was notified about via
   * a CREATE event.
   */
  long numCreated();

  /**
   * Returns the number of documents for which the publisher has been notified of successful completion.
   */
  long numSucceeded();

  /**
   * Returns the number of documents for which the publisher has been notified of a failure.
   */
  long numFailed();

  /**
   * Returns the number of documents for which the publisher has received a drop notification.
   */
  long numDropped();

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

  /**
   * Returns whether the publisher is paused: {@link #pause()} has been called and {@link #resume()} has not been
   * called since.
   */
  boolean isPaused();

  /**
   * Returns whether this publisher's Connector is running, so that it may still publish documents. True from when the
   * Connector's thread starts until it ends, including while publishing is paused or blocked on
   * {@code publisher.maxPendingDocs}.
   */
  boolean isPublishing();

  /**
   * Returns whether this publisher's Connector ran and has ended, so it will publish nothing more. Not the opposite of
   * {@link #isPublishing()}: before its Connector starts, a publisher is neither.
   *
   * <p> Finishing publishing does not mean the run is complete: documents may still be pending in the pipeline (see
   * {@link #hasPending()}).
   */
  boolean hasFinishedPublishing();

  /**
   * Indicates that the calling thread is done calling {@link #publish(Document)}, so that any per-thread resources the
   * Publisher keeps for it can be released.
   *
   * <p> Only needed by Connectors that publish from several threads: each of those threads should call it when it has
   * finished publishing. A single-threaded Connector does not need to call it.
   */
  void preClose();
}
