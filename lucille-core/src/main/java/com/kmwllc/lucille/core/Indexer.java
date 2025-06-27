package com.kmwllc.lucille.core;

import static com.kmwllc.lucille.core.Document.ID_FIELD;
import static com.kmwllc.lucille.core.Document.RUNID_FIELD;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.indexer.IndexerFactory;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.kmwllc.lucille.message.KafkaIndexerMessenger;
import com.kmwllc.lucille.util.LogUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import org.slf4j.MDC;
import org.slf4j.MDC.MDCCloseable;
import sun.misc.Signal;

/**
 * Indexes documents into a target destination.
 *
 * <p> Indexers have two separate configurations - the generic <code>indexer</code> configuration, and configuration for the
 * specific implementation held under a certain key. For example, <code>solr</code> holds configuration for a SolrIndexer, <code>csv</code>
 * holds configuration for a CSVIndexer, etc.
 *
 * <p> All implementations must declare a <code>public static Spec SPEC</code> defining the specific implementation's properties.
 * This Spec will be accessed reflectively in the super constructor, so the Indexer will not function without declaring a Spec.
 * The Config provided to <code>super()</code> will be validated against the Spec. The Indexer superclass will validate the
 * generic <code>indexer</code> configuration and the specific implementation configuration as well.
 *
 * <p> The <code>public static Spec SPEC</code> should <b>not</b> include the config key in your property names. For example, you
 * only write "url", not "solr.url", in a Spec for a SolrIndexer.
 *
 * <p> A {@link Spec#indexer()} does not include any default legal properties, as it is intended for use in specific Indexer
 * implementations.
 */
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

  /**
   * Creates the base implementation of an Indexer from the given parameters and validates it. Runs validation on the common "indexer"
   * config as well as the specific implementation's config, as either are present.
   *
   * <p> All Indexer implementations should declare a <code>public static final Spec SPEC</code> that defines the implementation's
   * specific properties. For example, Elasticsearch needs <code>index</code>, <code>url</code>, etc.
   *
   * @param config The root config for Lucille. (In other words, should potentially include both "indexer" and specific
   *               implementation config, like "solr" or "elasticsearch".)
   */
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

    // Validate the "indexer" entry and the specific implementation entry (using the spec) in the Config, if present.
    validateIndexerConfigs(config);
  }

  /**
   * Gets the key / parent name of this Indexer in a Config. For example, "elasticsearch" for ElasticsearchIndexer. Return null
   * if this Indexer does not take additional / specific configuration and, as such, has no config key.
   * @return the key / parent name of this Indexer in a Config.
   */
  protected abstract String getIndexerConfigKey();

  /**
   * Return true if connection to the destination search engine is valid and the relevant index or
   * collection exists; false otherwise.
   *
   * @return Whether this Indexer has a valid connection to its destination.
   */
  public abstract boolean validateConnection();

  /**
   * Send a batch of documents to the destination search engine. Implementations should use a single
   * call to the batch API provided by the search engine client, if available, as opposed to sending
   * each document individually.
   *
   * @param documents The documents to send to the destination.
   * @return A set of Pairs, containing the Documents that were not successfully indexed, along with a String of information about why it failed.
   * Returns an empty set if no documents fail, or if this information is not supported by the Indexer implementation. Does not return null.
   * @throws Exception In the event of a considerable error causing indexing to fail. (Does not throw
   * Exceptions just because some Documents were not successfully indexed.)
   */
  protected abstract Set<Pair<Document, String>> sendToIndex(List<Document> documents) throws Exception;

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

  /**
   * Runs the Indexer, having it check for documents a certain number of times.
   *
   * @param iterations The number of times to check for a document.
   */
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

    try {
      stopWatch.reset();
      stopWatch.start();
      Set<Pair<Document, String>> failedDocPairs = sendToIndex(batchedDocs);
      stopWatch.stop();
      histogram.update(stopWatch.getNanoTime() / batchedDocs.size());
      meter.mark(batchedDocs.size());

      if (!failedDocPairs.isEmpty()) {
        log.warn("{} Documents were not indexed successfully.", failedDocPairs.size());
      }

      // Mark all the documents in failedDoc as failed
      for (Pair<Document, String> pair : failedDocPairs) {
        try {
          messenger.sendEvent(pair.getLeft(), "FAILED: " + pair.getRight(), Event.Type.FAIL);
          docLogger.error("Sent failure message for doc {}. Reason: {}", pair.getLeft().getId(), pair.getRight());
        } catch (Exception e) {
          log.error("Couldn't send failure event for doc {}", pair.getLeft().getId(), e);
        }
      }

      Set<Document> failedDocs = failedDocPairs.stream().map(Pair::getLeft).collect(Collectors.toSet());

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
    } finally {
      // We always mark batches as completed, regardless of whether the whole batch failed, some docs failed, etc.
      try {
        messenger.batchComplete(batchedDocs);
      } catch (Exception e) {
        log.error("Error marking batch complete.", e);
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

  public Spec getImplementationSpec() {
    try {
      return (Spec) this.getClass().getDeclaredField("SPEC").get(null);
    } catch (Exception e) {
      throw new RuntimeException("Error accessing " + getClass() + " Spec. Is it publicly and statically available under \"SPEC\"?", e);
    }
  }

  private void validateIndexerConfigs(Config config) {
    // Validate the general "indexer" entry in the Config. (This Spec is same for all indexers.)
    Config indexerConfig = config.getConfig("indexer");
    Spec.withoutDefaults()
        .optionalString("type", "class", "idOverrideField", "indexOverrideField", "deletionMarkerField", "deletionMarkerFieldValue",
            "deleteByFieldField", "deleteByFieldValue", "versionType", "routingField")
        .optionalNumber("batchSize", "batchTimeout", "logRate")
        .optionalBoolean("sendEnabled")
        .optionalList("ignoreFields", new TypeReference<List<String>>(){})
        .validate(indexerConfig, "Indexer");

    // Validate the specific implementation in the config (solr, elasticsearch, csv, ...) if it is present / needed.
    // This spec is unique to the implementation and must be public + static under "SPEC".
    String indexerConfigKey = getIndexerConfigKey();

    if (indexerConfigKey != null && config.hasPath(indexerConfigKey)) {
      Config specificImplConfig = config.getConfig(indexerConfigKey);
      getImplementationSpec().validate(specificImplConfig, indexerConfigKey);
    }
  }

  public int getBatchCapacity() {
    return batch.getCapacity();
  }
}
