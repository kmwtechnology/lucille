package com.kmwllc.lucille.core;

import static com.kmwllc.lucille.core.Document.ID_FIELD;
import static com.kmwllc.lucille.core.Document.RUNID_FIELD;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.indexer.IndexerFactory;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.kmwllc.lucille.message.KafkaIndexerMessenger;
import com.kmwllc.lucille.util.FieldFilter;
import com.kmwllc.lucille.util.LogUtils;
import com.kmwllc.lucille.util.ThreadNameUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 * <p> A {@link SpecBuilder#indexer()} does not include any default legal properties, as it is intended for use in specific Indexer
 * implementations.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>indexer.idOverrideField (String, Optional) : Name of a document field whose value will be used as the document's ID
 *   in the destination, overriding {@link com.kmwllc.lucille.core.Document#ID_FIELD}. The document's internal ID is not modified.
 *   Has no effect in Indexer implementations that do not interact with a search engine (e.x. CSVIndexer).</li>
 *   <li>indexer.indexOverrideField (String, Optional) : Name of a document field whose value will be used as the destination
 *   index, overriding the default index specified in the Indexer's configuration. Not supported by CSVIndexer or
 *   ElasticSearchIndexer classes.</li>
 *   <li>indexer.blacklist (List&lt;String&gt;, Optional) : Fields to exclude from the document sent to the destination.</li>
 *   <li>indexer.whitelist (List&lt;String&gt;, Optional) : Only include these fields from the document when sending to the
 *   destination.</li>
 *   <li>indexer.batchSize (Integer, Optional) : Number of documents to accumulate before sending to the destination. Defaults to
 *   {@value #DEFAULT_BATCH_SIZE} or {@value #NO_BATCH_SIZE} if batchByteSize is set. If both are set, batches are flushed when
 *   either limit is reached.</li>
 *   <li>indexer.batchByteSize (Long, Optional) : Total size of documents accumulated before sending to the destination. Defaults to
 *   {@value #NO_BATCH_SIZE_BYTES}.</li>
 *   <li>indexer.batchTimeout (Integer, Optional) : the number of milliseconds (since the previous add or flush) beyond which the batch
 *   will be considered as expired. Defaults to {@value #DEFAULT_BATCH_TIMEOUT}.</li>
 *   <li>indexer.deletionMarkerField (String, Optional) : Field that, when set to indexer.deletionMarkerFieldValue, marks a document
 *   for deletion. Must be set together with indexer.deletionMarkerFieldValue.</li>
 *   <li>indexer.deletionMarkerFieldValue (String, Optional) : Value of indexer.deletionMarkerField that triggers deletion.
 *   Must be set together with indexer.deletionMarkerField.</li>
 *   <li>indexer.deleteByFieldField (String, Optional) : Field used to identify documents for deletion by query. Must be set
 *   together with indexer.deleteByFieldValue.</li>
 *   <li>indexer.deleteByFieldValue (String, Optional) : Field whose value specifies which documents to delete by query. Must be set
 *   together with indexer.deleteByFieldField.</li>
 *   <li>indexer.maxRetries (Integer, Optional) : Maximum number of times to retry a failed batch before treating all documents
 *   in the batch as failures. Retries are disabled by default; set to a positive integer to opt in. A value of 0 or less
 *   is rejected as invalid — remove the parameter entirely to disable retries.</li>
 *   <li>indexer.retryWaitDurationMs (Integer, Optional) : Initial wait duration in milliseconds before the first retry.
 *   Subsequent retries use exponential backoff, doubling the wait on each attempt. Only allowed when maxRetries is also set.
 *   Defaults to {@value #DEFAULT_RETRY_INITIAL_WAIT_DURATION_MS}.</li>
 *   <li>indexer.retryMaxWaitDurationMs (Long, Optional) : Maximum wait duration in milliseconds between retries.
 *   Caps the exponential backoff so it does not grow unbounded. Only allowed when maxRetries is also set.
 *   Defaults to {@value #DEFAULT_RETRY_MAX_WAIT_DURATION_MS}.</li>
 *   <li>indexer.retryRandomizationFactor (Double, Optional) : Randomization factor applied to the wait duration
 *   to add jitter and prevent thundering-herd effects. A value of 0.5 means the actual wait will be between
 *   50% and 150% of the computed backoff. Set to 0.0 to disable jitter. Only allowed when maxRetries is also set.
 *   Defaults to {@value #DEFAULT_RETRY_RANDOMIZATION_FACTOR}.</li>
 *   <li>indexer.retryableStatusCodes (List&lt;Integer&gt;, Optional) : HTTP status codes that should trigger a retry when thrown
 *   as an {@link IndexerRetryableException}. Failures with status codes not in this list, or plain {@link IndexerException}
 *   failures, will not be retried. Include the sentinel value {@code -1} to also retry failures where no HTTP status code
 *   is available (e.g. network timeouts). When this parameter is omitted, the default {@code [429, 503, -1]} is used.
 *   An empty list is rejected as invalid configuration. Only allowed when maxRetries is also set.</li>
 *   <li>indexer.versionType (String, Optional) : The type of versioning to use for indexing documents. Specific indexer implementations may support different version types. The default version type is implementation-specific.</li>
 *   <li>indexer.versionField (String, Optional) : The field name to use for versioning. This field must be present in the document and must be of a type that supports versioning with that specific indexer implementation (e.g., numeric, string).</li>
 *   <li>indexer.maxConcurrentBatches (Integer, Optional) : Maximum number of batches that may be in flight to the destination
 *   at once. Defaults to {@value #DEFAULT_MAX_CONCURRENT_BATCHES}, which sends each batch synchronously on the indexer thread.
 *   Values greater than 1 are only accepted by implementations whose {@link #supportsConcurrentSends()} returns true. See
 *   "Concurrent sends" below.</li>
 * </ul>
 *
 * <p> <b>Concurrent sends.</b> When indexer.maxConcurrentBatches is greater than 1, {@link #sendToIndex(List)} (including
 * any retries) runs on a pool of that many threads, while polling, batching, and every {@link IndexerMessenger} call stay on
 * the indexer thread. The following guarantees hold:
 * <ul>
 *   <li>Batches are completed (events sent, {@link IndexerMessenger#batchComplete(List)} called) in the order they were
 *   dispatched, so concurrency never lets a messenger that commits offsets on batch completion commit past an
 *   unfinished batch. This holds only when indexer.indexOverrideField is unset: with it, batches are kept per index and
 *   one index's batch, holding later offsets of a partition, can be flushed (and so dispatched and completed) before
 *   another index's batch holding earlier offsets of the same partition. That limitation also applies to synchronous
 *   sends, and in-order completion does not remove it.</li>
 *   <li>A batch is not dispatched while it shares a destination ID with a batch still in flight, so writes and deletes
 *   of the same document are applied in order. A document's destination IDs are its own (the idOverrideField value, or
 *   the document ID) and the document IDs of its children, recursively.</li>
 *   <li>A batch containing a delete-by-query document (see deleteByFieldField) is dispatched only when nothing else is in
 *   flight, and nothing is dispatched behind it until it completes.</li>
 *   <li>All in-flight batches are completed before {@link #run()} returns. An interrupt does not abandon them: it is
 *   held while the indexer thread waits and completes a batch, then restored so the next poll sees it.</li>
 *   <li>While the indexer thread waits for an in-flight batch, it calls {@link IndexerMessenger#keepAlive()} every
 *   second.</li>
 * </ul>
 */
public abstract class Indexer implements Runnable {

  public static final int DEFAULT_BATCH_SIZE = 100;
  public static final int NO_BATCH_SIZE = Integer.MAX_VALUE;
  public static final long NO_BATCH_SIZE_BYTES = Long.MAX_VALUE;
  public static final int DEFAULT_BATCH_TIMEOUT = 100;
  public static final int DEFAULT_RETRY_INITIAL_WAIT_DURATION_MS = 1000;
  public static final long DEFAULT_RETRY_MAX_WAIT_DURATION_MS = 30000;
  public static final double DEFAULT_RETRY_RANDOMIZATION_FACTOR = 0.5;
  public static final List<Integer> DEFAULT_RETRYABLE_STATUS_CODES = List.of(429, 503, IndexerRetryableException.UNKNOWN_STATUS_CODE);
  public static final int DEFAULT_MAX_CONCURRENT_BATCHES = 1;
  private static final long SEND_POOL_SHUTDOWN_TIMEOUT_MS = 10000;
  // How often the indexer thread calls IndexerMessenger.keepAlive() while it waits for an in-flight batch.
  static final long KEEP_ALIVE_INTERVAL_MS = 1000;
  private static final AtomicInteger SEND_POOL_INSTANCES = new AtomicInteger();

  private static final Logger log = LoggerFactory.getLogger(Indexer.class);
  private static final Logger docLogger = LoggerFactory.getLogger("com.kmwllc.lucille.core.DocLogger");
  private static final Logger failedDocLogger = LoggerFactory.getLogger("com.kmwllc.lucille.core.FailedDocuments");

  private final IndexerMessenger messenger;
  private final Batch batch;

  private volatile boolean running = true;

  private final int logSeconds;

  private final Meter meter;
  private final Histogram histogram;

  protected final String idOverrideField;
  protected final String indexOverrideField;

  protected final String deletionMarkerField;
  protected final String deletionMarkerFieldValue;
  protected final String deleteByFieldField;
  protected final String deleteByFieldValue;

  protected final FieldFilter fieldFilter;

  private Instant lastLog = Instant.now();

  // A runID for a local (local / test) run. Null if not in one of those modes / started independently.
  private final String localRunId;

  protected final boolean bypass;

  // Non-null only when indexer.maxRetries is configured; null means retries are disabled (the default).
  private final Retry retry;
  // Empty when retries are disabled; otherwise the set of HTTP status codes (and -1 for no-status) that trigger a retry.
  private final List<Integer> retryableStatusCodes;

  private final int maxConcurrentBatches;
  // Null when maxConcurrentBatches is 1; batches are then sent synchronously on the indexer thread.
  private final ExecutorService sendPool;
  // Batches dispatched to sendPool, oldest first. Only touched by the indexer thread.
  private final Deque<InFlightBatch> inFlight = new ArrayDeque<>();
  // Destination IDs of every batch in inFlight. Disjoint across batches by construction.
  private final Set<String> inFlightIds = new HashSet<>();

  // The outcome of one sendToIndex call (with retries): exactly one of failedDocPairs and error is meaningful.
  private record SendOutcome(Set<Pair<Document, Exception>> failedDocPairs, Throwable error, long elapsedNanos) {}

  private record InFlightBatch(List<Document> docs, Set<String> ids, Future<SendOutcome> outcome) {}

  private record AwaitedOutcome(SendOutcome outcome, boolean interrupted) {}

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
  public Indexer(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix, String localRunId) {
    this.messenger = messenger;
    this.bypass = bypass;
    this.idOverrideField =
        config.hasPath("indexer.idOverrideField")
            ? config.getString("indexer.idOverrideField")
            : null;
    this.indexOverrideField =
        config.hasPath("indexer.indexOverrideField")
            ? config.getString("indexer.indexOverrideField")
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

    int batchSize;
    long batchByteSize;

    if (config.hasPath("indexer.batchSize") || config.hasPath("indexer.batchByteSize")) {
      // when only one is set, the other is disabled so the configured one governs
      batchSize = config.hasPath("indexer.batchSize") ? config.getInt("indexer.batchSize") : NO_BATCH_SIZE;
      batchByteSize = config.hasPath("indexer.batchByteSize") ? config.getLong("indexer.batchByteSize") : NO_BATCH_SIZE_BYTES;
    } else {
      batchSize = DEFAULT_BATCH_SIZE;
      batchByteSize = NO_BATCH_SIZE_BYTES;
    }

    int batchTimeout =
        config.hasPath("indexer.batchTimeout")
            ? config.getInt("indexer.batchTimeout")
            : DEFAULT_BATCH_TIMEOUT;
    this.batch =
        (indexOverrideField == null)
            ? new SingleBatch(batchSize, batchByteSize, batchTimeout)
            : new MultiBatch(batchSize, batchByteSize, batchTimeout, indexOverrideField);
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
    this.meter = metrics.meter(metricsPrefix + ".indexer.docsIndexed");
    this.histogram = metrics.histogram(metricsPrefix + ".indexer.batchTimeOverSize");
    this.localRunId = localRunId;

    this.fieldFilter = new FieldFilter(config.getConfig("indexer"));

    if (!config.hasPath("indexer.maxRetries")) {
      if (config.hasPath("indexer.retryWaitDurationMs") || config.hasPath("indexer.retryableStatusCodes")
          || config.hasPath("indexer.retryMaxWaitDurationMs") || config.hasPath("indexer.retryRandomizationFactor")) {
        throw new IllegalArgumentException(
            "indexer.retryWaitDurationMs, indexer.retryMaxWaitDurationMs, indexer.retryRandomizationFactor, "
                + "and indexer.retryableStatusCodes require indexer.maxRetries to be set.");
      }
      this.retry = null;
      this.retryableStatusCodes = List.of();
    } else {
      int maxRetries = config.getInt("indexer.maxRetries");
      if (maxRetries <= 0) {
        throw new IllegalArgumentException(
            "indexer.maxRetries must be greater than 0 when specified. Remove the parameter to disable retries.");
      }
      int retryInitialWaitDurationMs = ConfigUtils.getOrDefault(config, "indexer.retryWaitDurationMs", DEFAULT_RETRY_INITIAL_WAIT_DURATION_MS);
      long retryMaxWaitDurationMs = config.hasPath("indexer.retryMaxWaitDurationMs")
          ? config.getLong("indexer.retryMaxWaitDurationMs") : DEFAULT_RETRY_MAX_WAIT_DURATION_MS;
      double retryRandomizationFactor = config.hasPath("indexer.retryRandomizationFactor")
          ? config.getDouble("indexer.retryRandomizationFactor") : DEFAULT_RETRY_RANDOMIZATION_FACTOR;
      if (!config.hasPath("indexer.retryableStatusCodes")) {
        this.retryableStatusCodes = DEFAULT_RETRYABLE_STATUS_CODES;
      } else {
        this.retryableStatusCodes = config.getIntList("indexer.retryableStatusCodes");
        if (this.retryableStatusCodes.isEmpty()) {
          throw new IllegalArgumentException(
              "indexer.retryableStatusCodes must not be empty when specified. "
                  + "Remove the parameter to use the default, or provide at least one status code.");
        }
      }
      this.retry = Retry.of("indexer-batch-retry", RetryConfig.<Set<Pair<Document, Exception>>>custom()
          .maxAttempts(maxRetries + 1) // maxAttempts includes the initial attempt
          .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(
              retryInitialWaitDurationMs, IntervalFunction.DEFAULT_MULTIPLIER,
              retryRandomizationFactor, retryMaxWaitDurationMs))
          .retryOnResult(result -> {
            // Retry the batch if any per-document failure has a retryable status code.
            @SuppressWarnings("unchecked")
            Set<Pair<Document, Exception>> pairs = (Set<Pair<Document, Exception>>) result;
            return pairs.stream().anyMatch(p ->
                p.getRight() instanceof IndexerRetryableException e
                    && retryableStatusCodes.contains(e.getStatusCode()));
          })
          .retryOnException(e -> {
            if (e instanceof IndexerRetryableException retryable) {
              return retryableStatusCodes.contains(retryable.getStatusCode());
            }
            // Plain IndexerException (non-HTTP failure) — do not retry
            return false;
          })
          .build());
      this.retry.getEventPublisher().onRetry(event -> {
        Throwable lastThrowable = event.getLastThrowable();
        log.warn("Retrying batch send (attempt {}/{}), waiting {}ms: {}",
            event.getNumberOfRetryAttempts(), maxRetries,
            event.getWaitInterval().toMillis(),
            lastThrowable != null ? lastThrowable.getMessage() : "per-document retryable failures");
      });
    }

    this.maxConcurrentBatches = getMaxConcurrentBatches(config);
    if (maxConcurrentBatches < 1) {
      throw new IllegalArgumentException("indexer.maxConcurrentBatches must be at least 1.");
    }
    if (maxConcurrentBatches > 1 && !supportsConcurrentSends()) {
      throw new IllegalArgumentException(getClass().getName() + " does not support concurrent sends; "
          + "indexer.maxConcurrentBatches must be 1 or unset.");
    }

    // Validate the "indexer" entry and the specific implementation entry (using the spec) in the Config, if present.
    validateIndexerConfigs(config);

    // ThreadPoolExecutor starts threads lazily, so an Indexer that is never run holds none.
    this.sendPool = maxConcurrentBatches == 1 ? null : Executors.newFixedThreadPool(maxConcurrentBatches,
        sendThreadFactory(localRunId));
  }

  /**
   * Returns the configured indexer.maxConcurrentBatches, or {@value #DEFAULT_MAX_CONCURRENT_BATCHES} when it is not set.
   * Destination client factories use this to size their connection pools.
   */
  public static int getMaxConcurrentBatches(Config config) {
    // getInt, unlike ConfigUtils.getOrDefault, converts a string value such as an environment substitution.
    return config.hasPath("indexer.maxConcurrentBatches")
        ? config.getInt("indexer.maxConcurrentBatches") : DEFAULT_MAX_CONCURRENT_BATCHES;
  }

  // Thread names are Lucille-<runId>-IndexerSend-<instance>-<n>; the instance number keeps them unique when several
  // indexers share a run.
  private static ThreadFactory sendThreadFactory(String runId) {
    int instance = SEND_POOL_INSTANCES.incrementAndGet();
    AtomicInteger count = new AtomicInteger();
    return task -> {
      Thread thread = new Thread(task,
          ThreadNameUtils.createName("IndexerSend-" + instance + "-" + count.incrementAndGet(), runId));
      thread.setDaemon(true);
      return thread;
    };
  }

  /**
   * Whether {@link #sendToIndex(List)} may be called from several threads at once. Implementations that return true
   * accept indexer.maxConcurrentBatches greater than 1. This method is called from the Indexer constructor, so it must not
   * depend on subclass state.
   *
   * @return true if this Indexer's sendToIndex is thread-safe; false (the default) otherwise.
   */
  protected boolean supportsConcurrentSends() {
    return false;
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
   * @return A set of Pairs containing Documents that were not successfully indexed, along with an
   *     Exception describing the failure. Use {@link IndexerRetryableException} to signal that the
   *     failure may be transient; the base class will trigger a retry of the entire batch for any
   *     returned exception whose status code is in the configured retryable set. Use
   *     {@link IndexerException} for permanent per-document failures. Returns an empty set if all
   *     documents succeeded or if per-document failure tracking is not supported. Does not return null.
   * @throws Exception for errors that prevent the entire batch from being attempted.
   */
  protected abstract Set<Pair<Document, Exception>> sendToIndex(List<Document> documents) throws Exception;

  /** Close the client or connection to the destination search engine. */
  public abstract void closeConnection();

  private void close() {
    if (sendPool != null) {
      shutdownSendPool();
    }
    if (messenger != null) {
      try {
        messenger.close();
      } catch (Exception e) {
        log.error("Error closing messenger", e);
      }
    }
    closeConnection();
  }

  // On the normal path every batch has been completed, so the pool is idle. If the indexer thread is exiting
  // abnormally (an Error), sends still in flight are interrupted.
  private void shutdownSendPool() {
    if (inFlight.isEmpty()) {
      sendPool.shutdown();
    } else {
      log.warn("Shutting down indexer with {} batches still in flight.", inFlight.size());
      sendPool.shutdownNow();
    }
    try {
      if (!sendPool.awaitTermination(SEND_POOL_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        log.warn("Indexer send pool did not terminate within {} ms.", SEND_POOL_SHUTDOWN_TIMEOUT_MS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
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
      completeAllInFlight();
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
      completeAllInFlight();
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

    if (sendPool != null) {
      completeFinishedInFlight();
    }

    if (batchedDocs.isEmpty()) {
      return;
    }

    if (sendPool == null) {
      completeBatch(batchedDocs, send(batchedDocs));
    } else {
      dispatch(batchedDocs);
    }
  }

  /**
   * Sends the batch to the destination, applying the retry policy. Runs on the indexer thread, or on the send pool when
   * maxConcurrentBatches is greater than 1. Never throws: any Throwable is captured in the outcome so that
   * {@link #completeBatch} can handle it on the indexer thread.
   */
  private SendOutcome send(List<Document> batchedDocs) {
    long start = System.nanoTime();
    try {
      // Note: the retry wraps the entire sendToIndex() call. If sendToIndex() partially succeeds
      // (e.g. some documents indexed before a subsequent delete-by-query fails), a retry will
      // re-execute the entire method. This is considered safe because search engine upserts are idempotent —
      // re-indexing an already-indexed document produces the same result.
      // When retries are configured, retryOnResult triggers a retry if any per-document failure has a
      // retryable status code. When retries are exhausted, the last result is returned directly —
      // preserving per-document detail for the FAIL event path in completeBatch.
      Set<Pair<Document, Exception>> failedDocPairs = retry != null
          ? Retry.decorateCheckedSupplier(retry, () -> sendToIndex(batchedDocs)).get()
          : sendToIndex(batchedDocs);
      return new SendOutcome(failedDocPairs, null, System.nanoTime() - start);
    } catch (Throwable t) {
      return new SendOutcome(null, t, System.nanoTime() - start);
    }
  }

  /**
   * Records metrics and sends FAIL / FINISH events for a sent batch, then marks it complete. Always runs on the indexer
   * thread, in the order batches were dispatched.
   */
  private void completeBatch(List<Document> batchedDocs, SendOutcome outcome) {
    try {
      if (outcome.error() != null) {
        throw outcome.error();
      }
      Set<Pair<Document, Exception>> failedDocPairs = outcome.failedDocPairs();
      histogram.update(outcome.elapsedNanos() / batchedDocs.size());
      meter.mark(batchedDocs.size());

      if (!failedDocPairs.isEmpty()) {
        log.warn("{} Documents were not indexed successfully.", failedDocPairs.size());
      }

      // Mark all the documents in failedDoc as failed
      for (Pair<Document, Exception> pair : failedDocPairs) {
        sendFailEvent(pair.getLeft(), pair.getRight().getMessage());
      }

      Set<Document> failedDocs = failedDocPairs.stream().map(Pair::getLeft).collect(Collectors.toSet());

      List<Document> succeededDocs = batchedDocs.stream()
          .filter(d -> !failedDocs.contains(d))
          .collect(Collectors.toList());

      try {
        messenger.sendEvents(succeededDocs, "SUCCEEDED", Event.Type.FINISH);
        for (Document d : succeededDocs) {
          try (MDCCloseable docIdMDC = MDC.putCloseable(ID_FIELD, d.getId())) {
            docLogger.info("Sent success message for doc {}.", d.getId());
          }
        }
      } catch (Exception e) {
        List<String> docIds = succeededDocs.stream().map(Document::getId).collect(Collectors.toList());
        log.error("Error sending completion events for docs {}. RUN WILL HANG.", docIds, e);
      }
    } catch (Throwable t) {
      // Rethrow Errors (OutOfMemoryError, etc.) — they should never be swallowed
      if (t instanceof Error) {
        throw (Error) t;
      }
      Exception e = (t instanceof Exception) ? (Exception) t : new RuntimeException(t);
      // If an Exception is thrown, there was some larger error causing nothing (or essentially nothing) to be indexed.
      // So everything is considered to have failed - we won't even look at failedDocs.
      log.error("Error sending documents to index: {}", e.getMessage(), e);

      for (Document d : batchedDocs) {
        sendFailEvent(d, e.getMessage());
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
   * Submits the batch to the send pool once it may run alongside the batches already in flight, completing the oldest
   * in-flight batches until it can. See "Concurrent sends" in the class javadoc for the rules.
   */
  private void dispatch(List<Document> batchedDocs) {
    Set<String> ids = new HashSet<>();
    for (Document doc : batchedDocs) {
      addDestinationIds(doc, ids);
    }
    boolean isBarrier = batchedDocs.stream().anyMatch(this::isDeleteByQuery);

    while (!inFlight.isEmpty()
        && (isBarrier || inFlight.size() >= maxConcurrentBatches || overlapsInFlight(ids))) {
      completeOldestInFlight();
    }

    Map<String, String> mdc = MDC.getCopyOfContextMap();
    Future<SendOutcome> outcome = sendPool.submit(() -> {
      if (mdc != null) {
        MDC.setContextMap(mdc);
      }
      try {
        return send(batchedDocs);
      } finally {
        MDC.clear();
      }
    });
    inFlight.addLast(new InFlightBatch(batchedDocs, ids, outcome));
    inFlightIds.addAll(ids);

    if (isBarrier) {
      completeAllInFlight();
    }
  }

  private boolean overlapsInFlight(Set<String> ids) {
    for (String id : ids) {
      if (inFlightIds.contains(id)) {
        return true;
      }
    }
    return false;
  }

  // Completes in-flight batches whose sends have finished, stopping at the first one that hasn't, so completion order
  // matches dispatch order.
  private void completeFinishedInFlight() {
    while (!inFlight.isEmpty() && inFlight.peekFirst().outcome().isDone()) {
      completeOldestInFlight();
    }
  }

  private void completeAllInFlight() {
    while (!inFlight.isEmpty()) {
      completeOldestInFlight();
    }
  }

  // Waits for the oldest in-flight batch's send to finish and completes it.
  //
  // The batch is completed with the interrupt status clear, because the messenger calls that complete it may block
  // (a queue put, a producer flush) and would otherwise throw at once, losing the batch's completion. Any interrupt,
  // whether it arrived before or during the wait, is restored afterwards so the next poll sees it.
  private void completeOldestInFlight() {
    InFlightBatch oldest = inFlight.peekFirst();
    // A send that has already finished never checks the flag, so clear it here rather than relying on the wait.
    boolean interrupted = Thread.interrupted();
    try {
      AwaitedOutcome awaited = awaitOutcome(oldest.outcome());
      interrupted |= awaited.interrupted();
      inFlight.removeFirst();
      inFlightIds.removeAll(oldest.ids());
      completeBatch(oldest.docs(), awaited.outcome());
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  // Waits without giving up on interruption: the batch has been sent (or is being sent) and must be accounted for.
  // Returns with the interrupt status clear, reporting whether an interrupt arrived. Calls messenger.keepAlive() every
  // KEEP_ALIVE_INTERVAL_MS while the send is unfinished, so a messenger that must poll to stay alive (such as a
  // consumer group member) survives a long wait.
  private AwaitedOutcome awaitOutcome(Future<SendOutcome> future) {
    boolean interrupted = false;
    while (true) {
      try {
        return new AwaitedOutcome(future.get(KEEP_ALIVE_INTERVAL_MS, TimeUnit.MILLISECONDS), interrupted);
      } catch (InterruptedException e) {
        interrupted = true;
      } catch (TimeoutException e) {
        keepAlive();
      } catch (ExecutionException e) {
        // send() captures its own Throwables, so this only happens if the task wrapper itself failed.
        return new AwaitedOutcome(new SendOutcome(null, e.getCause(), 0), interrupted);
      }
    }
  }

  private void keepAlive() {
    try {
      messenger.keepAlive();
    } catch (Exception e) {
      log.warn("Error in messenger keepAlive while waiting for an in-flight batch.", e);
    }
  }

  // The IDs a document is written under at the destination: its own (the idOverrideField value, or the document ID)
  // and, recursively, those of its children, which are written under their own document IDs. Batches sharing an ID are
  // never in flight together.
  private void addDestinationIds(Document doc, Set<String> ids) {
    String idOverride = getDocIdOverride(doc);
    ids.add(idOverride != null ? idOverride : doc.getId());
    addChildIds(doc, ids);
  }

  private static void addChildIds(Document doc, Set<String> ids) {
    if (!doc.hasChildren()) {
      return;
    }
    for (Document child : doc.getChildren()) {
      ids.add(child.getId());
      addChildIds(child, ids);
    }
  }

  // Whether the document is a delete-by-query request, which can match documents in any batch. Mirrors the checks in the
  // Solr, OpenSearch, and Elasticsearch indexers.
  private boolean isDeleteByQuery(Document doc) {
    return deletionMarkerField != null
        && deletionMarkerFieldValue != null
        && doc.hasNonNull(deletionMarkerField)
        && doc.getString(deletionMarkerField).equals(deletionMarkerFieldValue)
        && deleteByFieldField != null
        && doc.has(deleteByFieldField)
        && deleteByFieldValue != null
        && doc.has(deleteByFieldValue);
  }

  /**
   * Sends a FAIL event for the given document with the specified reason, setting up MDC context
   * for structured logging.
   */
  private void sendFailEvent(Document d, String reason) {
    try (MDCCloseable docIdMDC = MDC.putCloseable(ID_FIELD, d.getId())) {
      if (d.getRunId() != null) {
        MDC.pushByKey(RUNID_FIELD, d.getRunId());
      }
      messenger.sendEvent(d, "FAILED: " + reason, Event.Type.FAIL);
      docLogger.error("Document FAILED during indexing: {}. Reason: {}", d.getId(), reason);
      failedDocLogger.atError().setMessage(() -> d.toString()).log();
    } catch (Exception e) {
      docLogger.error("Couldn't send failure event for doc {}. RUN WILL HANG.", d.getId(), e);
    } finally {
      if (d.getRunId() != null) {
        MDC.popByKey(RUNID_FIELD);
      }
    }
  }

  /**
   * Returns the value of the configured idOverrideField on the given document, to be used as the
   * document's ID when sending to the index in place of {@link Document#ID_FIELD}.
   * Returns null if idOverrideField is not configured, or the document does not have that field.
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
    if (fieldFilter.isActive()) {
      indexerDoc.keySet().removeIf(key -> !fieldFilter.shouldInclude(key));
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
    SpecBuilder.withoutDefaults()
        .optionalString("type", "class", "idOverrideField", "indexOverrideField", "deletionMarkerField", "deletionMarkerFieldValue",
            "deleteByFieldField", "deleteByFieldValue", "versionType", "versionField", "routingField")
        .optionalNumber("batchSize", "batchByteSize", "batchTimeout", "logRate", "maxRetries", "retryWaitDurationMs",
            "retryMaxWaitDurationMs", "retryRandomizationFactor", "maxConcurrentBatches")
        .optionalBoolean("sendEnabled")
        .optionalList("whitelist", new TypeReference<List<String>>(){})
        .optionalList("blacklist", new TypeReference<List<String>>(){})
        .optionalList("retryableStatusCodes", new TypeReference<List<Integer>>(){}).build()
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
