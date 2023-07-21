package com.kmwllc.lucille.core;

import com.codahale.metrics.*;
import com.kmwllc.lucille.message.WorkerMessageManager;
import com.kmwllc.lucille.message.WorkerMessageManagerFactory;
import com.kmwllc.lucille.util.LogUtils;
import com.typesafe.config.Config;
import org.apache.commons.collections4.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

class Worker implements Runnable {

  public static final int TIMEOUT_CHECK_MS = 1000;
  public static final String METRICS_SUFFIX = ".worker.docProcessingTme";

  private static final Logger log = LoggerFactory.getLogger(Worker.class);
  private final WorkerMessageManager manager;

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

  public Worker(
      Config config, WorkerMessageManager manager, String pipelineName, String metricsPrefix)
      throws Exception {
    this.manager = manager;
    this.pipeline = Pipeline.fromConfig(config, pipelineName, metricsPrefix);
    if (config.hasPath("worker.maxRetries")) {
      log.info(
          "Retries will be tracked in Zookeeper with a configured maximum of: "
              + config.getInt("worker.maxRetries"));
      this.trackRetries = true;
      this.counter = new ZKRetryCounter(config);
    }
    this.pollInstant = new AtomicReference();
    this.pollInstant.set(Instant.now());
    this.metricsPrefix = metricsPrefix;
  }

  @Override
  public void run() {
    MetricRegistry metrics = SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG);
    Timer timer = metrics.timer(metricsPrefix + METRICS_SUFFIX);

    while (running) {
      Document doc;
      try {
        pollInstant.set(Instant.now());
        // blocking poll with a timeout which we assume to be in the range of
        // several milliseconds to several seconds
        doc = manager.pollDocToProcess();
      } catch (Exception e) {
        log.info("interrupted " + e);
        terminate();
        return;
      }

      if (doc == null) {
        commitOffsetsAndRemoveCounter(null);
        continue;
      }

      if (trackRetries && counter.add(doc)) {
        try {
          log.info(
              "Retry count exceeded for document " + doc.getId() + "; Sending to failure topic");
          manager.sendFailed(doc);
        } catch (Exception e) {
          log.error("Failed to send doc to failure topic: " + doc.getId(), e);
        }

        try {
          manager.sendEvent(doc, "SENT_TO_DLQ", Event.Type.FAIL);
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
          // BEFORE the input/parent document is completed. This prevents a situation where the
          // Runner
          // assumes the run is complete because the parent is complete and the Publisher didn't
          // know
          // about the children. This code assumes the pipeline emits children before parents.
          if (!doc.getId().equals(result.getId())) {
            manager.sendEvent(result, null, Event.Type.CREATE);
          }

          if (result.isDropped()) {
            manager.sendEvent(result, null, Event.Type.DROP);
          } else {
            // send the completed document to the queue for indexing
            manager.sendCompleted(result);
          }
        }

        context.stop();
      } catch (Exception e) {
        log.error("Error processing document: " + doc.getId(), e);
        try {
          manager.sendEvent(doc, null, Event.Type.FAIL);
        } catch (Exception e2) {
          log.error("Error sending failure event for document: " + doc.getId(), e2);
        }

        commitOffsetsAndRemoveCounter(doc);

        continue;
      }

      commitOffsetsAndRemoveCounter(doc);
    }

    // commit any remaining offsets before termination
    commitOffsetsAndRemoveCounter(null);

    try {
      manager.close();
    } catch (Exception e) {
      log.error("Error closing message manager", e);
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
      manager.commitPendingDocOffsets();
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

  public static WorkerThread startThread(
      Config config, WorkerMessageManager manager, String pipelineName, String metricsPrefix)
      throws Exception {
    Worker worker = new Worker(config, manager, pipelineName, metricsPrefix);
    WorkerThread workerThread = new WorkerThread(worker, config);
    workerThread.start();
    return workerThread;
  }

  public static void main(String[] args) throws Exception {
    Config config = ConfigUtils.loadConfig();
    String pipelineName = args.length > 0 ? args[0] : config.getString("worker.pipeline");
    log.debug("Starting Workers for pipeline: " + pipelineName);

    WorkerMessageManagerFactory workerMessageManagerFactory =
        WorkerMessageManagerFactory.getKafkaFactory(config, pipelineName);

    WorkerPool workerPool =
        new WorkerPool(config, pipelineName, workerMessageManagerFactory, pipelineName);
    workerPool.start();

    Signal.handle(
        new Signal("INT"),
        signal -> {
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
