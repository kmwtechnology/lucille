package com.kmwllc.lucille.core;

import static com.kmwllc.lucille.core.Document.ID_FIELD;
import static com.kmwllc.lucille.core.Document.RUNID_FIELD;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.kmwllc.lucille.indexer.IndexerFactory;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.kmwllc.lucille.message.KafkaIndexerMessenger;
import com.kmwllc.lucille.util.LogUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Set;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import org.slf4j.MDC;
import org.slf4j.MDC.MDCCloseable;
import sun.misc.Signal;

public abstract class Indexer implements Runnable {

  public static final int DEFAULT_BATCH_SIZE = 100;
  public static final int DEFAULT_BATCH_TIMEOUT = 100;

  private static final Logger log = LoggerFactory.getLogger(Indexer.class);
  private static final Logger docLogger = LoggerFactory.getLogger("com.kmwllc.lucille.core.DocLogger");

  private final IndexerMessenger messenger;
  private final Batch batch;

  private volatile boolean running = true;

  private final int logSeconds;

  private final StopWatch stopWatch;
  private final Meter meter;
  private final Histogram histogram;

  protected final String idOverrideField;
  protected final String indexOverrideField;

  protected final String deletionMarkerField;
  protected final String deletionMarkerFieldValue;
  protected final String deleteByFieldField;
  protected final String deleteByFieldValue;

  protected final List<String> ignoreFields;

  private Instant lastLog = Instant.now();

  // A runID for a local (local / test) run. Null if not in one of those modes / started independently.
  private final String localRunId;

  public void terminate() {
    running = false;
    log.debug("terminate");
  }

  public Indexer(Config config, IndexerMessenger messenger, String metricsPrefix, String localRunId) {
    this.messenger = messenger;
    this.idOverrideField =
        config.hasPath("indexer.idOverrideField")
            ? config.getString("indexer.idOverrideField")
            : null;
    this.indexOverrideField =
        config.hasPath("indexer.indexOverrideField")
            ? config.getString("indexer.indexOverrideField")
            : null;
    this.ignoreFields =
        config.hasPath("indexer.ignoreFields")
            ? config.getStringList("indexer.ignoreFields")
            : null;
    this.deletionMarkerField =
        config.hasPath("indexer.deletionMarkerField")
            ? config.getString("indexer.deletionMarkerField")
            : null;
    this.deletionMarkerFieldValue =
        config.hasPath("indexer.deletionMarkerFieldValue")
            ? config.getString("indexer.deletionMarkerFieldValue")
            : null;
    this.deleteByFieldField =
        config.hasPath("indexer.deleteByFieldField")
            ? config.getString("indexer.deleteByFieldField")
            : null;
    this.deleteByFieldValue =
        config.hasPath("indexer.deleteByFieldValue")
            ? config.getString("indexer.deleteByFieldValue")
            : null;
    int batchSize =
        config.hasPath("indexer.batchSize")
            ? config.getInt("indexer.batchSize")
            : DEFAULT_BATCH_SIZE;
    int batchTimeout =
        config.hasPath("indexer.batchTimeout")
            ? config.getInt("indexer.batchTimeout")
            : DEFAULT_BATCH_TIMEOUT;
    this.batch =
        (indexOverrideField == null)
            ? new SingleBatch(batchSize, batchTimeout)
            : new MultiBatch(batchSize, batchTimeout, indexOverrideField);
    // validate config deletionFields that must be present together
    if ((deleteByFieldField != null && deleteByFieldValue == null)
        || (deleteByFieldField == null && deleteByFieldValue != null)) {
      throw new IllegalArgumentException(
          "When one of indexer.deleteByFieldField and indexer.deleteByFieldValue are set, both must be set.");
    }
    if ((deletionMarkerField != null && deletionMarkerFieldValue == null)
      || (deletionMarkerField == null && deletionMarkerFieldValue != null)) {
      throw new IllegalArgumentException(
        "When one of indexer.deletionMarkerField and indexer.deletionMarkerFieldValue are set, both must be set.");
    }

    this.logSeconds = ConfigUtils.getOrDefault(config, "log.seconds", LogUtils.DEFAULT_LOG_SECONDS);
    MetricRegistry metrics = SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG);
    this.stopWatch = new StopWatch();
    this.meter = metrics.meter(metricsPrefix + ".indexer.docsIndexed");
    this.histogram = metrics.histogram(metricsPrefix + ".indexer.batchTimeOverSize");

    this.localRunId = localRunId;
  }

  /**
   * Return true if connection to the destination search engine is valid and the relevant index or
   * collection exists; false otherwise.
   */
  public abstract boolean validateConnection();

  /**
   * Send a batch of documents to the destination search engine. Implementations should use a single
   * call to the batch API provided by the search engine client, if available, as opposed to sending
   * each document individually.
   *
   * @return A set of the Documents that were not successfully indexed. Return an empty set if no documents fail / if not
   * supported by the Indexer implementation.
   * @throws Exception In the event of a considerable error causing indexing to fail. Does not throw
   * Exceptions just because some Documents may have not been indexed successfully.
   */
  protected abstract Set<Document> sendToIndex(List<Document> documents) throws Exception;

  /** Close the client or connection to the destination search engine. */
  public abstract void closeConnection();

  private void close() {
    if (messenger != null) {
      try {
        messenger.close();
      } catch (Exception e) {
        log.error("Error closing messenger", e);
      }
    }
    closeConnection();
  }

  @Override
  public void run() {
    if (localRunId == null) {
      MDC.pushByKey(RUNID_FIELD, "UNKNOWN");
    } else {
      MDC.pushByKey(RUNID_FIELD, localRunId);
    }

    try {
      while (running) {
        checkForDoc();
      }
      sendToIndexWithAccounting(batch.flush()); // handle final batch
    } finally {
      MDC.popByKey(RUNID_FIELD);
      close();
    }
  }

  public void run(int iterations) {
    try {
      for (int i = 0; i < iterations; i++) {
        checkForDoc();
      }
      sendToIndexWithAccounting(batch.flush()); // handle final batch
    } finally {
      close();
    }
  }

  private void checkForDoc() {
    Document doc;
    try {
      // blocking poll with a timeout which we assume to be in the range of
      // several milliseconds to several seconds
      doc = messenger.pollDocToIndex();
    } catch (Exception e) {
      log.info("Indexer interrupted ", e);
      terminate();
      return;
    }

    if (doc == null) {
      sendToIndexWithAccounting(batch.flushIfExpired());
    } else {
      try (MDCCloseable docIdMDC = MDC.putCloseable(ID_FIELD, doc.getId())) {
        docLogger.info("Indexer polled doc {}, added to batch.", doc.getId());
      }
      sendToIndexWithAccounting(batch.add(doc));
    }
  }

  private void sendToIndexWithAccounting(List<Document> batchedDocs) {
    if (ChronoUnit.SECONDS.between(lastLog, Instant.now()) > logSeconds) {
      log.info(
          String.format(
              "%d docs indexed. One minute rate: %.2f docs/sec. Mean backend latency: %.2f ms/doc.",
              meter.getCount(),
              meter.getOneMinuteRate(),
              histogram.getSnapshot().getMean() / 1000000));
      lastLog = Instant.now();
    }

    if (batchedDocs.isEmpty()) {
      return;
    }

    Set<Document> failedDocs;
    try {
      stopWatch.reset();
      stopWatch.start();
      failedDocs = sendToIndex(batchedDocs);
      stopWatch.stop();
      histogram.update(stopWatch.getNanoTime() / batchedDocs.size());
      meter.mark(batchedDocs.size());
    } catch (Exception e) {
      // If an Exception is thrown, there was some larger error causing nothing (or essentially nothing) to be indexed.
      // So everything is considered to have failed - we won't even look at failedDocs.
      log.error("Error sending documents to index: " + e.getMessage(), e);

      for (Document d : batchedDocs) {
        try (MDCCloseable docIdMDC = MDC.putCloseable(ID_FIELD, d.getId())) {
          if (d.getRunId() != null) {
            MDC.pushByKey(RUNID_FIELD, d.getRunId());
          }

          messenger.sendEvent(d, "FAILED: " + e.getMessage(), Event.Type.FAIL);
          docLogger.error("Sent failure message for doc {}.", d.getId());
        } catch (Exception e2) {
          // TODO: The run won't be able to finish if this event isn't received; can we do something
          // special here?
          log.error("Couldn't send failure event for doc {}", d.getId(), e2);
        } finally {
          if (d.getRunId() != null) {
            MDC.popByKey(RUNID_FIELD);
          }
        }
      }

      // We've sent a message for each Document. Don't want to run the code coming after finally.
      return;
    } finally {
      // We always mark batches as completed, regardless of if failedDocs isn't null / empty.
      try {
        messenger.batchComplete(batchedDocs);
      } catch (Exception e) {
        log.error("Error marking batch complete.", e);
      }
    }

    // to prevent multiple checks for null Set - just use an empty set by default.
    if (failedDocs == null) {
      failedDocs = Set.of();
    }

    if (!failedDocs.isEmpty()) {
      log.warn("{} Documents were not indexed successfully.", failedDocs.size());
    }

    // Mark all the documents in failedDoc as failed
    for (Document d : failedDocs) {
      try {
        messenger.sendEvent(d, "FAILED", Event.Type.FAIL);
        docLogger.error("Sent failure message for doc {}.", d.getId());
      } catch (Exception e) {
        log.error("Couldn't send failure event for doc {}", d.getId(), e);
      }
    }

    for (Document d : batchedDocs) {
      if (failedDocs.contains(d)) {
        continue;
      }

      try (MDCCloseable docIdMDC = MDC.putCloseable(ID_FIELD, d.getId())) {
        if (d.getRunId() != null) {
          MDC.pushByKey(RUNID_FIELD, d.getRunId());
        }

        messenger.sendEvent(d, "SUCCEEDED", Event.Type.FINISH);
        docLogger.info("Sent success message for doc {}.", d.getId());
      } catch (Exception e) {
        // TODO: The run won't be able to finish if this event isn't received; can we do something
        // special here?
        log.error("Error sending completion event for doc {}", d.getId(), e);
      } finally {
        if (d.getRunId() != null) {
          MDC.popByKey(RUNID_FIELD);
        }
      }
    }
  }

  /**
   * Returns the ID that should be sent to the destination index/collection for the given doc, in
   * place of the value of the Document.ID_FIELD field. Returns null if no override should be
   * applied for the given document.
   */
  protected String getDocIdOverride(Document doc) {
    if (idOverrideField != null && doc.has(idOverrideField)) {
      return doc.getString(idOverrideField);
    }
    return null;
  }

  /**
   * Returns the index that should be the destination for the given doc, in place of the default
   * index. Returns null if no index override should be applied for the given document.
   */
  protected String getIndexOverride(Document doc) {
    if (indexOverrideField != null && doc.has(indexOverrideField)) {
      return doc.getString(indexOverrideField);
    }
    return null;
  }

  protected Map<String, Object> getIndexerDoc(Document doc) {
    Map<String, Object> indexerDoc = doc.asMap();
    if (ignoreFields != null) {
      ignoreFields.forEach(indexerDoc::remove);
    }
    return indexerDoc;
  }

  public static void main(String[] args) throws Exception {
    Config config = ConfigFactory.load();
    String pipelineName = args.length > 0 ? args[0] : config.getString("indexer.pipeline");
    log.info("Starting Indexer for pipeline: " + pipelineName);
    IndexerMessenger messenger = new KafkaIndexerMessenger(config, pipelineName);
    Indexer indexer = IndexerFactory.fromConfig(config, messenger, false, pipelineName, null);

    if (!indexer.validateConnection()) {
      log.error("Indexer could not connect");
      System.exit(1);
    }

    Thread indexerThread = new Thread(indexer);
    indexerThread.start();

    Signal.handle(
        new Signal("INT"),
        signal -> {
          indexer.terminate();
          log.info("Indexer shutting down");
          try {
            indexerThread.join();
          } catch (InterruptedException e) {
            log.error("Interrupted", e);
          }
          System.exit(0);
        });
  }

  public int getBatchCapacity() {
    return batch.getCapacity();
  }
}
