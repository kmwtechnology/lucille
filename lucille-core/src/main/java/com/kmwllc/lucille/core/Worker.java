package com.kmwllc.lucille.core;

import static com.kmwllc.lucille.core.Document.ID_FIELD;
import static com.kmwllc.lucille.core.Document.RUNID_FIELD;

import com.codahale.metrics.*;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.message.HybridWorkerMessenger;
import com.kmwllc.lucille.message.KafkaWorkerMessenger;
import com.kmwllc.lucille.message.MessengerException;
import com.kmwllc.lucille.message.WorkerMessenger;
import com.kmwllc.lucille.util.LogUtils;
import com.kmwllc.lucille.util.ThrowableUtils;
import com.typesafe.config.Config;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Instant;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Polls a {@link WorkerMessenger} for documents, runs each one through the {@link Pipeline} and sends the results on
 * for indexing.
 *
 * <p>Failure handling, from least to most severe:
 * <ul>
 *   <li>A document that a stage cannot process gets a FAIL event, as does any child whose CREATE event was already
 *   sent but which never reached the indexer. The worker then commits and moves on to the next document. This covers
 *   Exceptions and the Errors that are local to one document, such as a StackOverflowError or a LinkageError from an
 *   optional dependency, and a send failure that the messenger marks as specific to the document (for Kafka, a record
 *   too large for the topic); see {@link MessengerException#isDocumentSpecific()}.</li>
 *   <li>Any other failure to send a document or an event through the messenger is fatal to the worker: the messenger
 *   can no longer be trusted, so no FAIL event is sent and the current offsets are not committed. The documents in
 *   flight are redelivered when a worker next consumes the partition.</li>
 *   <li>A failure to poll is fatal to the worker. Offsets of the documents processed so far are committed.</li>
 *   <li>An OutOfMemoryError, InternalError or UnknownError is fatal to the worker and is rethrown after cleanup, since
 *   the JVM itself may no longer be reliable. See {@link ThrowableUtils#isFatal}.</li>
 * </ul>
 * In every fatal case the failure is recorded, see {@link #getThrowable()}, so that a {@link WorkerPool} can fail the
 * run rather than wait for the connector timeout; the messenger is closed and the stages are stopped either way.
 */
class Worker implements Runnable {

  public static final Spec SPEC = SpecBuilder.withoutDefaults()
      .optionalString("pipeline")
      .optionalNumber("threads", "maxProcessingSecs", "maxRetries")
      .optionalBoolean("exitOnTimeout", "enableHeartbeat").build();

  public static final String METRICS_SUFFIX = ".worker.docProcessingTme";

  private static final Logger log = LoggerFactory.getLogger(Worker.class);
  private static final Logger docLogger = LoggerFactory.getLogger("com.kmwllc.lucille.core.DocLogger");
  private static final Logger failedDocLogger = LoggerFactory.getLogger("com.kmwllc.lucille.core.FailedDocuments");

  private final WorkerMessenger messenger;
  private final String localRunId;

  private final Pipeline pipeline;

  private volatile boolean running = true;

  // the Throwable that stopped this worker, if it stopped for any reason other than terminate()
  private volatile Throwable failure;

  private final AtomicReference<Instant> pollInstant;

  private boolean trackRetries = false;
  private RetryCounter counter = null;
  private final String metricsPrefix;

  public void terminate() {
    log.debug("terminate called");
    running = false;
  }

  public Worker(Config config, WorkerMessenger messenger, String localRunId, String pipelineName, String metricsPrefix)
      throws Exception {
    this.messenger = messenger;
    this.localRunId = localRunId;
    this.pipeline = Pipeline.fromConfig(config, pipelineName, metricsPrefix);
    if (config.hasPath("worker.maxRetries")) {
      // retries are redeliveries of a Kafka record; a local messenger never redelivers, so a counter would only cost a
      // ZooKeeper connection per worker thread
      if (messenger instanceof KafkaWorkerMessenger || messenger instanceof HybridWorkerMessenger) {
        log.info("Retries will be tracked in Zookeeper with a configured maximum of: " + config.getInt("worker.maxRetries"));
        this.trackRetries = true;
        this.counter = new ZKRetryCounter(config);
      } else {
        log.warn("worker.maxRetries is set but retries are only tracked for Kafka messengers; ignoring it.");
      }
    }
    this.pollInstant = new AtomicReference();
    this.pollInstant.set(Instant.now());
    this.metricsPrefix = metricsPrefix;
  }

  /**
   * For tests: as the public constructor, but with the retry counter supplied so that the dead letter path can be
   * exercised without ZooKeeper. The config must not set worker.maxRetries.
   */
  Worker(Config config, WorkerMessenger messenger, String localRunId, String pipelineName, String metricsPrefix,
      RetryCounter counter) throws Exception {
    this(config, messenger, localRunId, pipelineName, metricsPrefix);
    this.counter = counter;
    this.trackRetries = counter != null;
  }

  @Override
  public void run() {
    // localRunId is null in distributed mode, where the run id is put in the MDC per document below instead
    MDC.put(RUNID_FIELD, localRunId);

    MetricRegistry metrics = SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG);
    Timer timer = metrics.timer(metricsPrefix + METRICS_SUFFIX);

    // whether the offsets of documents polled so far may be committed when the worker exits; false once a document was
    // polled but could not be completed, so that it is redelivered rather than skipped
    boolean commitOnExit = true;
    Error rethrow = null;

    try {
      while (running) {
        Document doc;
        try {
          pollInstant.set(Instant.now());

          // blocking poll with a timeout which we assume to be in the range of
          // several milliseconds to several seconds
          doc = messenger.pollDocToProcess();

          // in distributed mode each document may belong to a different run, so tag the logs with its run id
          if (localRunId == null && doc != null) {
            MDC.put(RUNID_FIELD, doc.getRunId());
          }
        } catch (RecordDeserializationException e) {
          // already logged with the record's location by the messenger; the offsets of the records before it are
          // committed below, so a restarted worker resumes at the bad record (or past it, if configured to skip)
          log.error("Worker stopping: could not deserialize record {}@{}", e.topicPartition(), e.offset());
          failure = e;
          break;
        } catch (Exception e) {
          if (!running) {
            // interrupted or closed as part of a requested shutdown
            log.debug("Poll ended by shutdown", e);
            break;
          }
          log.error("Worker stopping: error polling for a document to process", e);
          failure = e;
          break;
        }

        if (doc == null) {
          commitOffsetsAndRemoveCounter(null);
          continue;
        }

        try (MDC.MDCCloseable docIdMDC = MDC.putCloseable(ID_FIELD, doc.getId())) {
          docLogger.info("Worker is processing document {}.", doc.getId());

          if (trackRetries && counter.add(doc)) {
            sendToDeadLetterQueue(doc);
            commitOffsetsAndRemoveCounter(doc);
            continue;
          }

          processDocument(doc, timer);
          commitOffsetsAndRemoveCounter(doc);
        }
      }
    } catch (MessengerException e) {
      failure = e;
      commitOnExit = false;
      log.error("Worker stopping: the messenger failed. The offsets of the document being processed are not committed, "
          + "so it will be redelivered.", e);
    } catch (Throwable t) {
      failure = t;
      commitOnExit = false;
      log.error("Worker stopping after a fatal error", t);
      if (t instanceof Error) {
        rethrow = (Error) t;
      }
    } finally {
      // This thread is about to end, so an interrupt (for example from the pool's exitOnTimeout watcher) has done its
      // job; clear it so that closing the messenger can complete, in particular so a Kafka consumer leaves its group
      // rather than making the broker wait out the session timeout before reassigning the partition.
      Thread.interrupted();

      // In local mode this worker belongs to one run, whose id was put in the MDC at the start, so the cleanup logs
      // below keep it. In distributed mode the MDC holds the run id of the last document processed, set per document
      // above; the cleanup belongs to no particular run, so remove it rather than attribute those logs to that run.
      if (localRunId == null) {
        MDC.remove(RUNID_FIELD);
      }

      if (commitOnExit) {
        // commit any remaining offsets before termination
        commitOffsetsAndRemoveCounter(null);
      }

      try {
        messenger.close();
      } catch (Exception e) {
        log.error("Error closing messenger", e);
      }

      try {
        pipeline.stopStages();
      } catch (Exception e) {
        log.error("Error stopping pipeline stage", e);
      }

      if (counter != null) {
        try {
          counter.close();
        } catch (Exception e) {
          log.error("Error closing retry counter", e);
        }
      }

      log.debug("Exiting");
    }

    if (rethrow != null) {
      throw rethrow;
    }
  }

  /**
   * Sends a document whose retries are exhausted to the dead letter queue and fails it. If the queue rejects the
   * document itself (for Kafka, a record too large for the fail topic) the document is still failed and committed,
   * since redelivering it would only repeat the rejection; any other failure to send propagates as a messenger failure.
   */
  private void sendToDeadLetterQueue(Document doc) throws MessengerException {
    docLogger.error("Document FAILED: retry count exceeded for {}. Sending to dead letter queue.", doc.getId());
    failedDocLogger.atError().setMessage(() -> doc.toString()).log();
    try {
      messenger.sendFailed(doc);
    } catch (MessengerException e) {
      if (!e.isDocumentSpecific()) {
        throw e;
      }
      docLogger.error("Document {} could not be sent to the dead letter queue.", doc.getId(), e);
    }
    messenger.sendEvent(doc, "SENT_TO_DLQ", Event.Type.FAIL);
  }

  /**
   * Runs one document through the pipeline and sends the results on. A problem with the document itself is handled
   * here by failing the document; a failure of the messenger, or of the JVM, propagates to the caller.
   */
  private void processDocument(Document doc, Timer timer) throws MessengerException {
    // a child whose CREATE event has been sent but which has not yet been sent for indexing or dropped; if processing
    // fails now, the publisher would wait for this child forever unless it is told the child failed
    Document announcedChild = null;

    try {
      Timer.Context context = timer.time();
      Iterator<Document> results = pipeline.processDocument(doc);

      while (results.hasNext()) {

        Document result = results.next();

        // if we're looking at a child document, send a CREATE event for it;
        // a document is a child if it has a different ID from the input document;
        // Note: we want to make sure that the Publisher is notified of any generated children
        // BEFORE the input/parent document is completed. This prevents a situation where the Runner
        // assumes the run is complete because the parent is complete and the Publisher didn't know
        // about the children. This code assumes the pipeline emits children before parents.
        if (!doc.getId().equals(result.getId())) {
          messenger.sendEvent(result, null, Event.Type.CREATE);
          announcedChild = result;
        }

        if (result.isDropped()) {
          messenger.sendEvent(result, null, Event.Type.DROP);
        } else {
          // send the completed document to the queue for indexing
          messenger.sendForIndexing(result);
        }
        announcedChild = null;
      }

      context.stop();
    } catch (MessengerException e) {
      if (!e.isDocumentSpecific()) {
        throw e;
      }
      // the messenger is fine; it is this document (or child) that cannot be sent
      failDocument(doc, announcedChild, e);
    } catch (Throwable t) {
      if (t instanceof Error e && ThrowableUtils.isFatal(e)) {
        // best effort: the publisher may still learn the document failed, but nothing can be relied on after this
        try {
          failDocument(doc, announcedChild, e);
        } catch (Throwable ignored) {
          // the original error is what matters
        }
        throw e;
      }
      // an Exception from a stage, or an Error local to this document such as StackOverflowError or a LinkageError
      failDocument(doc, announcedChild, t);
    }
  }

  /**
   * Records that the document failed in the pipeline and sends FAIL events for it and for the given child, if any.
   * A failure to send those events is a messenger failure and propagates as such.
   */
  private void failDocument(Document doc, Document announcedChild, Throwable cause) throws MessengerException {
    docLogger.error("Document FAILED during pipeline processing: {}", doc.getId(), cause);
    failedDocLogger.atError().setMessage(() -> doc.toString()).log();
    if (announcedChild != null) {
      Document child = announcedChild;
      docLogger.error("Child document {} of {} FAILED: its parent failed after the child was announced.", child.getId(),
          doc.getId());
      messenger.sendEvent(child, null, Event.Type.FAIL);
    }
    messenger.sendEvent(doc, null, Event.Type.FAIL);
  }

  public void logMetrics() {
    pipeline.logMetrics();
  }

  private void commitOffsetsAndRemoveCounter(Document doc) {
    try {
      messenger.commitPendingDocOffsets();
      if (trackRetries && doc != null) {
        counter.remove(doc);
      }
    } catch (Exception commitException) {
      log.error("Error committing updated offsets for pending documents", commitException);
    }
  }

  public AtomicReference<Instant> getPreviousPollInstant() {
    return pollInstant;
  }

  /**
   * @return The Throwable that stopped this worker, or null if it is running or stopped because {@link #terminate()}
   * was called.
   */
  public Throwable getThrowable() {
    return failure;
  }

  public static WorkerThread startThread(Worker worker, String name) {
    WorkerThread workerThread = new WorkerThread(worker, name);
    workerThread.start();
    return workerThread;
  }

  /**
   * Starts a pool of workers reading from Kafka for the pipeline named by the first argument (or by
   * <code>worker.pipeline</code>). The process runs until it is told to stop (SIGINT or SIGTERM); if a worker fails,
   * the remaining workers are stopped and the process exits 1. The body of this entry point is
   * {@link Launch#worker}.
   */
  public static void main(String[] args) throws Exception {
    Launch.worker(args);
  }

}
