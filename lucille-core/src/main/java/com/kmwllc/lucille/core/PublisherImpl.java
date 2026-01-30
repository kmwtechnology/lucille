package com.kmwllc.lucille.core;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.kmwllc.lucille.message.PublisherMessenger;
import com.kmwllc.lucille.util.LogUtils;
import com.typesafe.config.Config;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
import org.apache.commons.collections4.Bag;
import org.apache.commons.collections4.bag.HashBag;
import org.apache.commons.collections4.bag.SynchronizedBag;
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
 */
public class PublisherImpl implements Publisher {

  private static final Logger log = LoggerFactory.getLogger(PublisherImpl.class);
  private static final Logger docLogger = LoggerFactory.getLogger("com.kmwllc.lucille.core.DocLogger");

  private final PublisherMessenger messenger;

  private final String runId;

  private final String pipelineName;

  private final int logSeconds;

  // the actual number of documents sent for processing, which may be smaller
  // than the number of calls to publish() if isCollapsing==true
  private AtomicLong numPublished = new AtomicLong(0);

  private long numCreated = 0;
  private long numFailed = 0;
  private long numSucceeded = 0;
  private long numDropped = 0;

  private Instant start;
  private final Timer timer;
  private final ThreadLocal<Timer.Context> timerContext = ThreadLocal.withInitial(() -> null);
  private final boolean isCollapsing;
  private Document previousDoc = null;
  private volatile StopWatch firstDocStopWatch;

  private final Integer maxPendingDocs;

  // lock and condition used for blocking publishing when the number of pending docs exceeds the specified maxPendingDocs
  private final ReentrantLock lockForPendingDocs = new ReentrantLock();
  private final Condition pendingDocsBelowMaxCondition = lockForPendingDocs.newCondition();

  // lock and condition used for blocking publishing when pause() is called
  private final ReentrantLock lockForPauseResume = new ReentrantLock();
  private volatile Condition resumeCondition = null;

  // Bag of published documents that have not reached a terminal state. Also includes children of published documents.
  // Note that this is a Bag, not a Set, because if two documents with the same ID are published, we would
  // expect to receive two separate terminal events relating to those documents, and we will therefore make
  // two attempts to remove the ID. Upon each removal attempt, we would like there to be something present
  // to remove; otherwise we would classify the event as an "early" terminal event and treat it specially.
  // Also note that a Publisher may be shared by a Runner and a Connector: the connector may be publishing
  // new Documents while the Connector is receiving Events and calling handleEvent().
  // publish() and handleEvent() both update docIdsToTrack so the bag should be synchronized.
  private final Bag<String> docIdsToTrack = SynchronizedBag.synchronizedBag(new HashBag<>());

  // Bag of child documents for which a terminal event has been received early, before the corresponding CREATE event
  private final Bag<String> docIdsIndexedBeforeTracking = SynchronizedBag.synchronizedBag(new HashBag<>());

  public PublisherImpl(Config config, PublisherMessenger messenger, String runId,
      String pipelineName, String metricsPrefix, boolean isCollapsing) throws Exception {
    this.messenger = messenger;
    this.runId = runId;
    this.pipelineName = pipelineName;
    this.timer =
        SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG).timer(metricsPrefix + ".timeBetweenPublishCalls");
    this.isCollapsing = isCollapsing;
    this.logSeconds = ConfigUtils.getOrDefault(config, "log.seconds", LogUtils.DEFAULT_LOG_SECONDS);

    Integer maxPendingDocs = ConfigUtils.getOrDefault(config, "publisher.maxPendingDocs", null);
    if (maxPendingDocs != null && maxPendingDocs < 0) {
      maxPendingDocs = null;
    }
    this.maxPendingDocs = maxPendingDocs;

    messenger.initialize(runId, pipelineName);
    this.firstDocStopWatch = new StopWatch();
    this.firstDocStopWatch.start();
  }

  public PublisherImpl(Config config, PublisherMessenger messenger, String runId,
      String pipelineName) throws Exception {
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

  /**
   * Submits the given document for processing by any available pipeline worker.
   *
   * Stamps the current Run ID on the document and begins "tracking" events relating the document.
   *
   * IMPORTANT: After calling publish, code should not update the Document or read values from it since at this point
   * it may have been picked up by a worker thread.
   *
   * Thread-safety: multiple threads may safely call publish() on the same Publisher instance, except when
   * the Publisher is in collapsing mode (isCollapsing=true)
   */
  @Override
  public void publish(Document document) throws Exception {

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
          log.info("First doc published after " + firstDocStopWatch.getTime(TimeUnit.MILLISECONDS) + " ms");
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
    docIdsToTrack.add(docId);

    try {
      messenger.sendForProcessing(document);
    } catch (Exception e) {
      // we assume that if an exception was encountered here, the doc was not actually made available for processing,
      // and that we won't be receiving any Events relating to it, so we can stop tracking its docId now
      docIdsToTrack.remove(docId, 1);
      throw e;
    }

    numPublished.incrementAndGet();
  }

  @Override
  public void flush() throws Exception {
    if (previousDoc != null) {
      sendForProcessing(previousDoc);
    }
    previousDoc = null;
  }

  @Override
  public void preClose() throws Exception {
    if (timerContext.get() != null) {
      timerContext.get().stop();
      timerContext.remove();
    }
  }

  @Override
  public void close() throws Exception {
    if (timerContext.get() != null) {
      timerContext.get().stop();
      timerContext.remove();
    }
    messenger.close();
  }

  @Override
  public void handleEvent(Event event) {
    String docId = event.getDocumentId();
    MDC.put(Document.ID_FIELD, docId);
    docLogger.info("Publisher is handling an event ({}) for doc {}.", event.getType(), docId);

    if (event.isCreate()) {

      numCreated++;

      // if we're learning that a child document has been created, we need to begin tracking it unless
      // we have already received an early confirmation that it was indexed
      // TODO: this does not handle redundant create events
      if (!docIdsIndexedBeforeTracking.remove(docId, 1)) {
        docIdsToTrack.add(docId);
      }

    } else {

      if (Event.Type.FINISH.equals(event.getType())) {
        numSucceeded++;
      } else if (Event.Type.FAIL.equals(event.getType())) {
        numFailed++;
      } else if (Event.Type.DROP.equals(event.getType())) {
        numDropped++;
      }

      // if we're learning that a document has finished processing, or failed, we can stop tracking it;
      // but if we weren't previously tracking it, we need to remember that we've seen it so that
      // if we receive an out-of-order or late create event for this document in the future,
      // we won't start tracking it then
      if (!docIdsToTrack.remove(docId, 1)) {
        docIdsIndexedBeforeTracking.add(docId);
      } else {
        if (maxPendingDocs != null) {
          try {
            lockForPendingDocs.lock();
            // since we have removed a docID from the bag of docIdsToTrack (and this is the main place where we do this),
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

    MDC.remove(Document.ID_FIELD);
  }

  @Override
  public PublisherResult waitForCompletion(ConnectorThread thread, int timeout) throws Exception {
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
      Event event = messenger.pollEvent();

      if (event != null) {
        handleEvent(event);
      }

      if (timeout > 0 && ChronoUnit.MILLIS.between(start, Instant.now()) > timeout) {
        log.error("Exiting run with " + numPending() + " pending documents; connector timed out (" + timeout + "ms)");
        return new PublisherResult(false, "Connector timeout.");
      }

      // stop waiting if the connector threw an exception
      if (thread.hasException()) {
        log.error("Exiting run with " + numPending() + " pending documents; connector threw exception", thread.getException());
        return new PublisherResult(false, "Connector exception.");
      }

      // We are done if 1) the Connector thread has terminated and therefore no more Documents will be generated,
      // 2) all published Documents and their children are accounted for (none are pending),
      // 3) there are no more Events relating to the current run to consume
      // Regarding 3), we assume there are no more events if the previous call to messenger.pollEvent() returned null
      // In a Kafka deployment, the publisher should be the only consumer of the event topic, and the topic should
      // have a single partition
      if (!thread.isAlive() && !hasPending() && event == null) {
        if (timerContext.get() != null) {
          timerContext.get().stop();
        }
        String collapseInfo = "";
        if (isCollapsing && numPublished.get() < timer.getCount()) {
          collapseInfo = String.format(" (%d after collapsing)", numPublished.get());
        }
        // Did not replace the following String.format with interpolation due to unique formatting (".2f")
        log.info(String.format("Publisher complete. Mean publishing rate: %.2f docs/sec. Mean connector latency: %.2f ms/doc.",
            timer.getMeanRate(), timer.getSnapshot().getMean() / 1000000));
        log.info("{} docs published{}. {} children created. {} success events. {} failure events. {} drop events.",
            timer.getCount(), collapseInfo, numCreated, numSucceeded, numFailed, numDropped);
        if (numPublished.get() > 0 && numFailed == 0) {
          log.info("All documents SUCCEEDED.");
        }
        if (numFailed > 0) {
          log.error(numFailed + " documents FAILED, but run will continue.");
        }
        return new PublisherResult(!thread.hasException(), null);
      }

      if (ChronoUnit.SECONDS.between(lastLog, Instant.now()) > logSeconds) {
        if (thread.isAlive()) {
          log.info(String.format(
              "%d docs published. One minute rate: %.2f docs/sec. Mean connector latency: %.2f ms/doc. Waiting on %d docs.",
              timer.getCount(), timer.getOneMinuteRate(), timer.getSnapshot().getMean() / 1000000, numPending()));
        } else {
          log.info("Connector complete. Waiting on {} docs.", numPending());
        }
        lastLog = Instant.now();
      }
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
  public long numCreated() {
    return numCreated;
  }

  @Override
  public long numSucceeded() {
    return numSucceeded;
  }

  @Override
  public long numFailed() {
    return numFailed;
  }

  @Override
  public long numDropped() {
    return numDropped;
  }

  public Integer getMaxPendingDocs() {
    return maxPendingDocs;
  }
}
