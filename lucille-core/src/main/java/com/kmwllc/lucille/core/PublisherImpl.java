package com.kmwllc.lucille.core;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.message.PublisherMessenger;
import com.kmwllc.lucille.util.LogUtils;
import com.typesafe.config.Config;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Publisher implementation that maintains an in-memory list of pending documents.
 *
 * Note that this implementation includes an explicit design decision that the publisher should
 * not remember all of the documents it has published because there could be an unbounded number of such documents
 * and they could take up significant space in memory. We don't even want the publisher to remember
 * all of the ids of those documents. As soon as the publisher learns that a document has reached
 * an end-state in the workflow, it can "forget" about that document aside from including it
 * a count of how many documents were published. The intention is that the publisher will be receiving
 * events in a separate thread from the one that is publishing documents (e.g. handleEvent() and publish() will
 * be called by separate threads) so that the publisher can stop tracking some documents even as it starts
 * tracking new ones. To achieve a memory of all the documents that were published,
 * a persisting implementation of PublisherMessenger could be given to the Publisher at construction time,
 * and the history could be accessed from the messenger.
 *
 * <p> This class is internal to Lucille: the Runner uses it to track a run and wait for it to complete. Connectors
 * use only the {@link Publisher} interface, and code outside Lucille should not depend on the methods this class
 * adds to it.
 */
public class PublisherImpl implements Publisher {

  public static final Spec SPEC = SpecBuilder.withoutDefaults()
      .optionalNumber("queueCapacity", "maxPendingDocs").build();

  private static final Logger log = LoggerFactory.getLogger(PublisherImpl.class);
  private static final Logger docLogger = LoggerFactory.getLogger("com.kmwllc.lucille.core.DocLogger");

  private final PublisherMessenger messenger;

  private final String runId;

  private final String pipelineName;

  private final int logSeconds;

  // the number of times a call to publish() was completed across all publishing threads
  private final AtomicLong numReceived = new AtomicLong(0);

  // the actual number of documents sent for processing, which may be smaller
  // than the number of calls to publish() if isCollapsing==true
  private final AtomicLong numPublished = new AtomicLong(0);

  // outcome counts, updated by the event-handling thread and readable from any thread
  private final AtomicLong numCreated = new AtomicLong(0);
  private final AtomicLong numFailed = new AtomicLong(0);
  private final AtomicLong numSucceeded = new AtomicLong(0);
  private final AtomicLong numDropped = new AtomicLong(0);

  private Instant start;
  private final Timer timer;
  private final ThreadLocal<Timer.Context> timerContext = ThreadLocal.withInitial(() -> null);
  private final boolean isCollapsing;
  private Document previousDoc = null;
  // set while a publish() or flush() call is in progress in collapsing mode, so that overlapping calls can be rejected
  private final AtomicBoolean collapsingPublishInProgress = new AtomicBoolean(false);
  private volatile StopWatch firstDocStopWatch;

  private final Integer maxPendingDocs;

  // set by close(); a connector that keeps publishing after the run has ended gets an exception instead of feeding
  // documents into a system that no longer tracks them
  private volatile boolean closed = false;

  // the thread running this publisher's Connector, once the Runner has attached it
  private volatile ConnectorThread connectorThread;

  // lock and condition used for blocking publishing when the number of pending docs exceeds the specified maxPendingDocs
  private final ReentrantLock lockForPendingDocs = new ReentrantLock();
  private final Condition pendingDocsBelowMaxCondition = lockForPendingDocs.newCondition();

  // lock and condition used for blocking publishing when pause() is called
  private final ReentrantLock lockForPauseResume = new ReentrantLock();
  private volatile Condition resumeCondition = null;

  // Map of published document IDs that have not reached a terminal state. Also tracks children.
  // Uses ConcurrentHashMap for lock-free concurrent access from multiple publishing threads and
  // the event-handling thread. The AtomicInteger value is used as a mutable int counter (its
  // atomicity is not relied upon — all access is within compute() which holds the bin lock).
  // If two documents with the same ID are published (e.g. create + update in streaming mode,
  // or at-least-once redelivery), we expect to receive separate terminal events for each, so
  // each add increments and each remove decrements. The entry is evicted when the count reaches zero.
  //
  // Note that a Publisher may be shared by a Runner and a Connector: the connector may be publishing
  // new Documents (via publish()) while the Runner is receiving Events and calling handleEvent().
  // These operations run in separate threads and both update docIdsToTrack.
  private final ConcurrentHashMap<String, AtomicInteger> docIdsToTrack = new ConcurrentHashMap<>();

  // Map of child document IDs for which a terminal event (FINISH/FAIL/DROP) has been received
  // early, before the corresponding CREATE event. When a CREATE event later arrives for such a
  // docId, we check this map first: if present, we decrement rather than adding to docIdsToTrack,
  // since the document has already completed and doesn't need tracking.
  private final ConcurrentHashMap<String, AtomicInteger> docIdsIndexedBeforeTracking = new ConcurrentHashMap<>();

  public PublisherImpl(Config config, PublisherMessenger messenger, String runId,
      String pipelineName, String metricsPrefix, boolean isCollapsing) throws PublisherException {
    this.messenger = messenger;
    this.runId = runId;
    this.pipelineName = pipelineName;
    this.timer =
        SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG).timer(metricsPrefix + ".timeBetweenPublishCalls");
    this.isCollapsing = isCollapsing;
    this.logSeconds = ConfigUtils.getOrDefault(config, "log.seconds", LogUtils.DEFAULT_LOG_SECONDS);

    // zero or a negative value means there is no limit; a literal limit of zero would block the first publish forever
    Integer maxPendingDocs = ConfigUtils.getIntOrDefault(config, "publisher.maxPendingDocs", null);
    if (maxPendingDocs != null && maxPendingDocs <= 0) {
      maxPendingDocs = null;
    }
    this.maxPendingDocs = maxPendingDocs;

    try {
      messenger.initialize(runId, pipelineName);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PublisherException("Interrupted while initializing publisher. (" + pipelineName + ")", e);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new PublisherException("Error initializing publisher. (" + pipelineName + ")", e);
    }
    this.firstDocStopWatch = new StopWatch();
    this.firstDocStopWatch.start();
  }

  public PublisherImpl(Config config, PublisherMessenger messenger, String runId,
      String pipelineName) throws PublisherException {
    this(config, messenger, runId, pipelineName, "default", false);
  }

  @Override
  public void pause() {
    try {
      lockForPauseResume.lock();
      if (resumeCondition != null) {
        throw new IllegalStateException();
      }
      // populate a Condition which publish() will check for and wait on
      resumeCondition = lockForPauseResume.newCondition();
    } finally {
      lockForPauseResume.unlock();
    }
  }

  @Override
  public void resume() {
    try {
      lockForPauseResume.lock();
      if (resumeCondition == null) {
        throw new IllegalStateException();
      }
      // signal any threads waiting on the resumeCondition
      resumeCondition.signalAll();
      // set resumeCondition to null so that future calls to publish() will not find a Condition to wait on
      resumeCondition = null;
    } finally {
      lockForPauseResume.unlock();
    }
  }

  @Override
  public boolean isPaused() {
    return resumeCondition != null;
  }

  /**
   * Submits the given document for processing by any available pipeline worker.
   *
   * Stamps the current Run ID on the document and begins "tracking" events relating the document.
   *
   * IMPORTANT: After calling publish, code should not update the Document or read values from it since at this point
   * it may have been picked up by a worker thread.
   *
   * Thread-safety: multiple threads may safely call publish() on the same Publisher instance, except when
   * the Publisher is in collapsing mode (isCollapsing=true): there, overlapping calls to publish() or flush() throw
   * IllegalStateException.
   */
  @Override
  public void publish(Document document) throws PublisherException {
    beginCollapsingCall();
    try {
      publishAndTrack(document);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PublisherException("Interrupted while waiting to publish document " + document.getId(), e);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new PublisherException("Error publishing document " + document.getId(), e);
    } finally {
      endCollapsingCall();
    }
  }

  // collapsing combines consecutive documents, which is only meaningful when they are published one at a time;
  // publish() and flush() both read and replace previousDoc, so neither may overlap the other
  private void beginCollapsingCall() {
    if (isCollapsing && !collapsingPublishInProgress.compareAndSet(false, true)) {
      throw new IllegalStateException("A collapsing Publisher does not support overlapping calls to publish() or "
          + "flush(). (" + pipelineName + ")");
    }
  }

  private void endCollapsingCall() {
    if (isCollapsing) {
      collapsingPublishInProgress.set(false);
    }
  }

  private void publishAndTrack(Document document) throws Exception {
    checkNotClosed();

    // check if we should wait until resume() is called
    if (resumeCondition != null) {
      try {
        lockForPauseResume.lock();
        // double check that resumeCondition is set now that we have the lock
        if (resumeCondition != null) {
          // wait to be signalled via a call to resume()
          while (resumeCondition != null) { // use a loop to handle rare but possible "spurious wakeups"
            resumeCondition.await();
          }
        }
      } finally {
        lockForPauseResume.unlock();
      }
    }

    if (maxPendingDocs != null) {
      try {
        lockForPendingDocs.lock();
        // if the number of pending docs is higher than the specified max,
        // wait to be notified by the event handling thread when the size falls below the max;
        // retest the condition every 10 seconds to handle any possible edge cases where the notification is not received
        while (docIdsToTrack.size() >= maxPendingDocs) {
          pendingDocsBelowMaxCondition.await(10, TimeUnit.SECONDS);
          // the run may have ended while we were waiting for room
          checkNotClosed();
          // note that in a scenario where N threads are calling publish() and the number of pending docs reaches maxPendingDocs,
          // all N threads will block on docIdsToTracBelowMax.await();
          // when numPending eventually drops to maxPendingDocs-1 via handleEvent(), all N threads are signalled simultaneously;
          // each one may then proceed to publish a document, causing N pending docs to be added;
          // in this scenario, the number of pending docs may exceed maxPendingDocs by N-1;
          // since each thread will block the next time it calls publish() (assuming no events are handled that reduce numPending in
          // the interim) it should not be possible for numPending to exceed maxPendingDocs by more than N-1
        }
      } finally {
        lockForPendingDocs.unlock();
      }
    }

    // The runId (in MDC) is already set by the ConnectorThread calling publish.
    MDC.put(Document.ID_FIELD, document.getId());
    docLogger.info("Publishing document {}.", document.getId());

    // "double-checked locking" -- perform null check outside synchronized block first to avoid overhead of
    // synchronizing when not necessary
    if (firstDocStopWatch != null) {
      synchronized (this) {
        if (firstDocStopWatch != null) {
          firstDocStopWatch.stop();
          log.info("First doc published after {} ms", firstDocStopWatch.getTime(TimeUnit.MILLISECONDS));
          firstDocStopWatch = null;
        }
      }
    }
    if (timerContext.get() != null) {
      // stop timing the duration since the last call to publish;
      // the goal is to track the rate of publish() calls as well as the
      // average lag between them (which tells us how fast the connector produces each document)
      timerContext.get().stop();
    }
    try {
      publishInternal(document);
    } finally {
      timerContext.set(timer.time());
      MDC.remove(Document.ID_FIELD);
    }

    numReceived.incrementAndGet();
  }

  private void publishInternal(Document document) throws Exception {
    if (!isCollapsing) {
      sendForProcessing(document);
      return;
    }

    if (previousDoc == null) {
      previousDoc = document;
      return;
    }

    if (previousDoc.getId().equals(document.getId())) {
      previousDoc.setOrAddAll(document);
    } else {
      sendForProcessing(previousDoc);
      previousDoc = document;
    }
  }

  private void sendForProcessing(Document document) throws Exception {
    document.initializeRunId(runId);

    // capture the docId before we make the document available for update by other threads
    String docId = document.getId();

    // It is important to add the docId to docIdsToTrack before sending it for processing, not after.
    // As soon as the document has been sent for processing, the publisher could begin receiving Events relating to that document.
    // If the publisher quickly receives a DROP event, for example, we want to be sure that the docId has already been
    // added to docIdsToTrack so that it can be found there and removed, not mistakenly added to docIdsIndexedBeforeTracking
    docIdsToTrack.compute(docId, (k, count) -> {
      if (count == null) return new AtomicInteger(1);
      count.incrementAndGet();
      return count;
    });

    try {
      messenger.sendForProcessing(document);
    } catch (Exception e) {
      // we assume that if an exception was encountered here, the doc was not actually made available for processing,
      // and that we won't be receiving any Events relating to it, so we can stop tracking its docId now
      decrementOrRemove(docIdsToTrack, docId);
      throw e;
    }

    numPublished.incrementAndGet();
  }

  /**
   * Publish any documents that the Publisher might not have published immediately when publish() was called.
   * This applies mainly to a "collapsing mode" where the Publisher combines consecutive documents having the same
   * ID into a single document. In such a mode, the Publisher would not be able to immediately publish each
   * incoming document because the next might have the same ID and the two should be collapsed. So the Publisher
   * would have to save the previous document and only publish it if the _next_ document has a different ID.
   * At the end of the input, the publisher would be holding on to one previous document and flush() would need
   * to be called to make sure it gets published. It then flushes the messenger, so that documents it has buffered
   * are delivered and a failed asynchronous send is reported.
   */
  @Override
  public void flush() throws PublisherException {
    beginCollapsingCall();
    try {
      checkNotClosed();
      if (previousDoc != null) {
        sendForProcessing(previousDoc);
      }
      previousDoc = null;
      messenger.flush();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PublisherException("Interrupted while flushing publisher. (" + pipelineName + ")", e);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new PublisherException("Error flushing publisher. (" + pipelineName + ")", e);
    } finally {
      endCollapsingCall();
    }
  }

  @Override
  public void preClose() {
    if (timerContext.get() != null) {
      timerContext.get().stop();
      timerContext.remove();
    }
  }

  /**
   * Closes any connections opened by the publisher.
   *
   */
  public void close() {
    closed = true;
    if (timerContext.get() != null) {
      timerContext.get().stop();
      timerContext.remove();
    }
    messenger.close();
  }

  private void checkNotClosed() {
    if (closed) {
      throw new IllegalStateException("Publisher is closed; the run it belonged to has ended. (" + pipelineName + ")");
    }
  }

  /**
   * Updates internal accounting based on a received Event: if we learn that a document has
   * been indexed, we can stop tracking it. If we learn that a child document has been
   * newly created, we must start tracking it. If we learn that a child document has been
   * indexed before we began tracking it (i.e. before we received its CREATE event) we need
   * to store its ID separately so that when the CREATE event is eventually received,
   * we'll know NOT to begin tracking it.
   *
   */
  public void handleEvent(Event event) {
    String docId = event.getDocumentId();
    // the MDC entry only decorates this info-level line, so skip the put/remove when it is disabled
    boolean docLogEnabled = docLogger.isInfoEnabled();
    if (docLogEnabled) {
      MDC.put(Document.ID_FIELD, docId);
      docLogger.info("Publisher is handling an event ({}) for doc {}.", event.getType(), docId);
    }

    if (event.isCreate()) {

      numCreated.incrementAndGet();

      // if we're learning that a child document has been created, we need to begin tracking it unless
      // we have already received an early confirmation that it was indexed
      // TODO: this does not handle redundant create events
      if (!decrementOrRemove(docIdsIndexedBeforeTracking, docId)) {
        docIdsToTrack.compute(docId, (k, count) -> {
          if (count == null) return new AtomicInteger(1);
          count.incrementAndGet();
          return count;
        });
      }

    } else {

      if (Event.Type.FINISH.equals(event.getType())) {
        numSucceeded.incrementAndGet();
      } else if (Event.Type.FAIL.equals(event.getType())) {
        numFailed.incrementAndGet();
      } else if (Event.Type.DROP.equals(event.getType())) {
        numDropped.incrementAndGet();
      }

      // if we're learning that a document has finished processing, or failed, we can stop tracking it;
      // but if we weren't previously tracking it, we need to remember that we've seen it so that
      // if we receive an out-of-order or late create event for this document in the future,
      // we won't start tracking it then
      if (!decrementOrRemove(docIdsToTrack, docId)) {
        docIdsIndexedBeforeTracking.compute(docId, (k, count) -> {
          if (count == null) return new AtomicInteger(1);
          count.incrementAndGet();
          return count;
        });
      } else {
        if (maxPendingDocs != null) {
          try {
            lockForPendingDocs.lock();
            // since we have removed a docID from docIdsToTrack (and this is the main place where we do this),
            // check to see if the number of pending docs has fallen below the specified max and
            // notify any threads that are waiting on that condition; specifically, notify the connector thread(s)
            // where publish() is being called
            if (docIdsToTrack.size() < maxPendingDocs) {
              pendingDocsBelowMaxCondition.signalAll();
            }
          } finally {
            lockForPendingDocs.unlock();
          }
        }
      }
    }

    if (docLogEnabled) {
      MDC.remove(Document.ID_FIELD);
    }
  }

  /**
   * Waits until all published Documents and their children are complete and no more Documents are being published.
   *
   *
   * @param thread a reference to the Connector thread that is generating new Documents;
   *               this thread will be tested for its status via isAlive() to determine when the Connector
   *               has finished and no more Documents will be published
   * @param timeout number of milliseconds to wait, after which the method will return false
   * @throws PublisherException if events cannot be read from the messaging infrastructure, or the thread is
   *     interrupted while waiting for them (its interrupt flag is restored)
   */
  public PublisherResult waitForCompletion(ConnectorThread thread, int timeout) throws PublisherException {
    start = Instant.now();
    Instant lastLog = Instant.now();

    // poll for Events relating the current run; loop until all work is complete;
    // Note: this polling loop could be refactored so that we would 1) start a logging thread,
    // 2) start a Connector thread, 3) start an Event handling thread which would poll for events
    // in a fully blocking way, without a timeout, testing the termination conditions only after processing
    // each event. We would then join on the connector thread, then the Event handling thread, and
    // finally stop the logging thread.
    while (true) {

      // we assume that messenger.pollEvent() is a blocking operation with a timeout in the range
      // of several milliseconds to several seconds.
      // We want to avoid a busy wait; at the same time, we want to test the termination conditions of the loop
      // periodically even when there are no available events to process
      Event event = pollEvent();

      if (event != null) {
        handleEvent(event);
      }

      if (timeout > 0 && ChronoUnit.MILLIS.between(start, Instant.now()) > timeout) {
        log.error("Exiting run with {} pending documents; connector timed out ({}ms)", numPending(), timeout);
        return new PublisherResult(false, "Connector timeout.");
      }

      // stop waiting if the connector threw an exception, or another component aborted it (for example a worker died)
      if (thread.isFailed()) {
        String message = thread.getFailureMessage() != null ? thread.getFailureMessage() : "Connector exception.";
        log.error("Exiting run with {} pending documents; {}", numPending(), message, thread.getThrowable());
        return new PublisherResult(false, message);
      }

      // We are done if 1) the Connector thread has terminated and therefore no more Documents will be generated,
      // 2) all published Documents and their children are accounted for (none are pending),
      // 3) there are no more Events relating to the current run to consume
      // Regarding 3), we assume there are no more events if the previous call to messenger.pollEvent() returned null
      // In a Kafka deployment, the publisher should be the only consumer of the event topic, and the topic should
      // have a single partition
      if (!thread.isAlive() && !hasPending() && event == null) {
        // note: the present waitForCompletion() method usually executes in a separate thread from publish()
        // so this ThreadLocal timerContext is usually different from the one set by publish() and may not be set at all;
        // we stop it here just in case it is set
        if (timerContext.get() != null) {
          timerContext.get().stop();
        }
        String collapseInfo = "";
        if (isCollapsing && numPublished.get() < numReceived.get()) {
          collapseInfo = String.format(" (%d after collapsing)", numPublished.get());
        }
        // Did not replace the following String.format with interpolation due to unique formatting (".2f")
        log.info(String.format("Publisher complete. Mean publishing rate: %.2f docs/sec. Mean connector latency: %.2f ms/doc.",
            timer.getMeanRate(), timer.getSnapshot().getMean() / 1000000));
        log.info("{} docs published{}. {} children created. {} success events. {} failure events. {} drop events.",
            numReceived.get(), collapseInfo, numCreated.get(), numSucceeded.get(), numFailed.get(), numDropped.get());
        if (numPublished.get() > 0 && numFailed.get() == 0) {
          log.info("All documents SUCCEEDED.");
        }
        if (numFailed.get() > 0) {
          log.error("{} documents FAILED, but run will continue.", numFailed.get());
        }
        return new PublisherResult(!thread.isFailed(), null);
      }

      if (ChronoUnit.SECONDS.between(lastLog, Instant.now()) > logSeconds) {
        if (thread.isAlive()) {
          log.info(String.format(
              "%d docs published. One minute rate: %.2f docs/sec. Mean connector latency: %.2f ms/doc. Waiting on %d docs.",
              numReceived.get(), timer.getOneMinuteRate(), timer.getSnapshot().getMean() / 1000000, numPending()));
        } else {
          log.info("Connector complete. Waiting on {} docs.", numPending());
        }
        lastLog = Instant.now();
      }
    }
  }

  private Event pollEvent() throws PublisherException {
    try {
      return messenger.pollEvent();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PublisherException("Interrupted while waiting for events. (" + pipelineName + ")", e);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new PublisherException("Error polling for events. (" + pipelineName + ")", e);
    }
  }

  @Override
  public boolean hasPending() {
    return !docIdsToTrack.isEmpty();
  }

  @Override
  public long numPending() {
    return docIdsToTrack.size();
  }

  @Override
  public long numPublished() {
    return numPublished.get();
  }

  @Override
  public long numReceived() {
    return numReceived.get();
  }

  @Override
  public long numCreated() {
    return numCreated.get();
  }

  @Override
  public long numSucceeded() {
    return numSucceeded.get();
  }

  @Override
  public long numFailed() {
    return numFailed.get();
  }

  @Override
  public long numDropped() {
    return numDropped.get();
  }

  @Override
  public Integer getMaxPendingDocs() {
    return maxPendingDocs;
  }

  @Override
  public boolean isPublishing() {
    ConnectorThread thread = connectorThread;
    return thread != null && thread.isAlive();
  }

  @Override
  public boolean hasFinishedPublishing() {
    ConnectorThread thread = connectorThread;
    // not !isAlive(): a thread that has been attached but not yet started is not alive either
    return thread != null && thread.getState() == Thread.State.TERMINATED;
  }

  /**
   * Attaches the thread that runs this publisher's Connector, for {@link #isPublishing()} and
   * {@link #hasFinishedPublishing()}. Called by the Runner before it starts the thread.
   */
  void setConnectorThread(ConnectorThread thread) {
    this.connectorThread = thread;
  }

  /**
   * Atomically decrements the count for the given key in the map and removes the entry if the
   * count reaches zero. Returns true if the key was present (and decremented), false otherwise.
   *
   * Uses compute() to hold the bin lock for the key during the entire read-modify-write, preventing
   * a concurrent increment from interleaving between the decrement and the removal.
   *
   * Called from publish threads (error path in sendForProcessing) and the event-handling thread
   * (handleEvent). Concurrent calls on different keys are fully safe (ConcurrentHashMap). Concurrent
   * calls on the same key are safe because compute() serializes them per bin.
   */
  private static boolean decrementOrRemove(ConcurrentHashMap<String, AtomicInteger> map, String key) {
    // single-element array allows the lambda to communicate a result back to the caller,
    // since Java lambdas cannot assign to local variables from the enclosing scope
    boolean[] wasPresent = {false};
    map.compute(key, (k, count) -> {
      if (count == null) return null;
      wasPresent[0] = true;
      return count.decrementAndGet() <= 0 ? null : count;
    });
    return wasPresent[0];
  }
}
