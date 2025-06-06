package com.kmwllc.lucille.core;

import static com.kmwllc.lucille.core.Document.ID_FIELD;
import static com.kmwllc.lucille.core.Document.RUNID_FIELD;

import com.codahale.metrics.*;
import com.kmwllc.lucille.message.WorkerMessenger;
import com.kmwllc.lucille.message.WorkerMessengerFactory;
import com.kmwllc.lucille.util.LogUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import sun.misc.Signal;

import java.time.Instant;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

class Worker implements Runnable {

  public static final String METRICS_SUFFIX = ".worker.docProcessingTme";

  private static final Logger log = LoggerFactory.getLogger(Worker.class);
  private static final Logger docLogger = LoggerFactory.getLogger("com.kmwllc.lucille.core.DocLogger");

  private final WorkerMessenger messenger;
  private final String localRunId;

  private final Pipeline pipeline;

  private volatile boolean running = true;

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
      log.info("Retries will be tracked in Zookeeper with a configured maximum of: " + config.getInt("worker.maxRetries"));
      this.trackRetries = true;
      this.counter = new ZKRetryCounter(config);
    }
    this.pollInstant = new AtomicReference();
    this.pollInstant.set(Instant.now());
    this.metricsPrefix = metricsPrefix;
  }

  @Override
  public void run() {
    // Again, the runID here will be null in a non-local mode.
    MDC.put(RUNID_FIELD, localRunId);

    MetricRegistry metrics = SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG);
    Timer timer = metrics.timer(metricsPrefix + METRICS_SUFFIX);

    while (running) {
      Document doc;
      try {
        pollInstant.set(Instant.now());
        // blocking poll with a timeout which we assume to be in the range of
        // several milliseconds to several seconds

        doc = messenger.pollDocToProcess();

        // continuously update the MDC if we haven't been given a localRunID (in Kafka modes).
        if (localRunId == null && doc != null) {
          MDC.put(RUNID_FIELD, doc.getRunId());
        }
      } catch (Exception e) {
        log.info("interrupted " + e);
        terminate();
        return;
      }

      if (doc == null) {
        commitOffsetsAndRemoveCounter(null);
        continue;
      }

      try (MDC.MDCCloseable docIdMDC = MDC.putCloseable(ID_FIELD, doc.getId())) {
        docLogger.info("Worker is processing document {}.", doc.getId());

        if (trackRetries && counter.add(doc)) {
          try {
            log.info("Retry count exceeded for document " + doc.getId() + "; Sending to failure topic");
            messenger.sendFailed(doc);
          } catch (Exception e) {
            log.error("Failed to send doc to failure topic: " + doc.getId(), e);
          }

          try {
            messenger.sendEvent(doc, "SENT_TO_DLQ", Event.Type.FAIL);
          } catch (Exception e) {
            log.error("Failed to send completion event for: " + doc.getId(), e);
          }

          commitOffsetsAndRemoveCounter(doc);
          continue;
        }

        try {
          Timer.Context context = timer.time();
          Iterator<Document> results = pipeline.processDocument(doc);

          while (results.hasNext()) {

            Document result = results.next();

            // if we're looking at a child document, send a CREATE events for it;
            // a document is a child if it has a different ID from the input document;
            // Note: we want to make sure that the Publisher is notified of any generated children
            // BEFORE the input/parent document is completed. This prevents a situation where the Runner
            // assumes the run is complete because the parent is complete and the Publisher didn't know
            // about the children. This code assumes the pipeline emits children before parents.
            if (!doc.getId().equals(result.getId())) {
              messenger.sendEvent(result, null, Event.Type.CREATE);
            }

            if (result.isDropped()) {
              messenger.sendEvent(result, null, Event.Type.DROP);
            } else {
              // send the completed document to the queue for indexing
              messenger.sendForIndexing(result);
            }
          }

          context.stop();
        } catch (Exception e) {
          log.error("Error processing document: " + doc.getId(), e);
          try {
            messenger.sendEvent(doc, null, Event.Type.FAIL);
          } catch (Exception e2) {
            log.error("Error sending failure event for document: " + doc.getId(), e2);
          }

          commitOffsetsAndRemoveCounter(doc);

          continue;
        }

        commitOffsetsAndRemoveCounter(doc);
      }
    }

    // Don't have a run id attached to the logs below if in a non-local mode.
    if (localRunId == null) {
      MDC.remove(RUNID_FIELD);
    }

    // commit any remaining offsets before termination
    commitOffsetsAndRemoveCounter(null);

    try {
      messenger.close();
    } catch (Exception e) {
      log.error("Error closing messenger", e);
    }

    try {
      pipeline.stopStages();
    } catch (StageException e) {
      log.error("Error stopping pipeline stage", e);
    }

    log.debug("Exiting");
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

  public static WorkerThread startThread(Worker worker, String name) {
    WorkerThread workerThread = new WorkerThread(worker, name);
    workerThread.start();
    return workerThread;
  }

  public static void main(String[] args) throws Exception {
    Config config = ConfigFactory.load();
    String pipelineName = args.length > 0 ? args[0] : config.getString("worker.pipeline");
    log.debug("Starting Workers for pipeline: " + pipelineName);

    WorkerMessengerFactory workerMessengerFactory =
        WorkerMessengerFactory.getKafkaFactory(config, pipelineName);

    WorkerPool workerPool = new WorkerPool(config, pipelineName, null, workerMessengerFactory, pipelineName);
    workerPool.start();

    Signal.handle(new Signal("INT"), signal -> {
      workerPool.stop();
      log.info("Workers shutting down");
      try {
        workerPool.join();
      } catch (InterruptedException e) {
        log.error("Interrupted", e);
      }
      System.exit(0);
    });
  }

}
