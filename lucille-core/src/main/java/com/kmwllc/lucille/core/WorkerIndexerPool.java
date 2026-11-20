package com.kmwllc.lucille.core;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.kmwllc.lucille.message.HybridWorkerMessenger;
import com.kmwllc.lucille.message.KafkaConsumerMetrics;
import com.kmwllc.lucille.util.LogUtils;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;

public class WorkerIndexerPool {

  /**
   * Suffix of the Gauge, registered while the pool is running, whose name is the pipeline name followed by this suffix.
   * Its value is the lowest remaining capacity of any WorkerIndexer pair's indexer queue: 0 means at least one worker is
   * blocked waiting for its indexer.
   */
  public static final String INDEXER_QUEUE_REMAINING_CAPACITY_SUFFIX = ".hybrid.indexerQueue.remainingCapacity";

  public static final int DEFAULT_POOL_SIZE = 1;

  private static final Logger log = LoggerFactory.getLogger(WorkerPool.class);

  private final List<WorkerIndexer> workerIndexers = new ArrayList<>();
  // the indexer queue of each pair, read by the remaining-capacity gauge and the status log line
  private final List<BlockingQueue<Document>> indexerQueues = new ArrayList<>();

  private final Config config;
  private final String pipelineName;
  private Integer numWorkers = null;
  private boolean started = false;
  private final int logSeconds;
  private Timer logTimer;
  private final boolean bypassSearchEngine;
  private final Set<String> idSet;

  public WorkerIndexerPool(Config config, String pipelineName, boolean bypassSearchEngine,
      Set<String> idSet) {
    this.config = config;
    this.pipelineName = pipelineName;
    this.bypassSearchEngine = bypassSearchEngine;
    try {
      this.numWorkers = Pipeline.getIntProperty(config, pipelineName, "threads");
    } catch (PipelineException e) {
      log.error("Error reading pipeline config", e);
    }
    if (this.numWorkers == null) {
      this.numWorkers = ConfigUtils.getIntOrDefault(config, "worker.threads", DEFAULT_POOL_SIZE);
    }
    this.logSeconds = ConfigUtils.getOrDefault(config, "log.intervalSecs", LogUtils.DEFAULT_LOG_SECONDS);
    this.idSet = idSet;
  }

  public void start() throws Exception {
    if (started) {
      throw new IllegalStateException("WorkerIndexerPool can be started at most once");
    }
    started = true;
    log.info("Starting {} WorkerIndexer thread pairs for pipeline {}", numWorkers, pipelineName);

    for (int i = 0; i < numWorkers; i++) {
      try {
        WorkerIndexer workerIndexer = new WorkerIndexer();
        workerIndexer.start(config, pipelineName, bypassSearchEngine, idSet);
        workerIndexers.add(workerIndexer);
      } catch (Exception e) {
        log.error("Exception caught when starting WorkerIndexer thread {}; aborting", i+1);
        try {
          stop();
        } catch (Exception e2) {
          log.error("Exception caught when attempting to stop WorkerIndexer threads because of a startup problem", e2);
        }
        throw e;
      }
    }

    registerIndexerQueueGauge();

    // Timer to log a status message every minute
    // the Timer should use a daemon thread so it doesn't prevent the jvm from exiting
    logTimer = new Timer("WorkerIndexerPoolTimer", true);
    logTimer.schedule(new TimerTask() {
      @Override
      public void run() {
        log.info(statusMessage(true));
      }
    }, logSeconds * 1000, logSeconds * 1000);

  }

  public void stop() throws Exception {
    log.debug("Stopping {} worker threads", workerIndexers.size());
    if (logTimer != null) {
      logTimer.cancel();
    }
    // the gauge holds the pairs' queues; remove it so it does not report on (or keep) queues that are no longer in use
    SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG).remove(indexerQueueGaugeName());
    for (WorkerIndexer workerIndexer : workerIndexers) {
      // stop each WorkerIndexer, one at a time;
      // make sure the indexer is stopped before the worker, so that the
      // worker has a chance to commit the offsets of the batches the indexer
      // completed right before it stopped
      workerIndexer.stop();
    }
    if (!workerIndexers.isEmpty()) {
      // a final summary, after the last commits; the queues are no longer in use, so their capacity is left out
      log.info("WorkerIndexerPool for pipeline {} stopped. {}", pipelineName, statusMessage(false));
    }
    // tell one of the threads to log its metrics;
    // the output should be the same for any thread;
    // all threads get their metrics via a shared registry using the same naming scheme,
    // so the metrics are collected across all the threads
    if (!workerIndexers.isEmpty()) {
      workerIndexers.get(0).getWorker().logMetrics();
    }
  }

  private String indexerQueueGaugeName() {
    return pipelineName + INDEXER_QUEUE_REMAINING_CAPACITY_SUFFIX;
  }

  private void registerIndexerQueueGauge() {
    for (WorkerIndexer workerIndexer : workerIndexers) {
      indexerQueues.add(workerIndexer.getIndexerQueue());
    }
    MetricRegistry metrics = SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG);
    // registering a name twice throws; replace a gauge left behind by a pool for this pipeline that was not stopped
    metrics.remove(indexerQueueGaugeName());
    metrics.register(indexerQueueGaugeName(), (Gauge<Integer>) () -> minRemainingCapacity(indexerQueues));
  }

  /**
   * The status line logged every log.intervalSecs and, without the queues' remaining capacity, when the pool stops:
   * documents processed and pipeline latency, the workers' Kafka poll and commit metrics, and how often the indexer
   * queue was full. All of it covers every WorkerIndexer pair for the pipeline in this JVM.
   */
  String statusMessage(boolean includeRemainingCapacity) {
    MetricRegistry metrics = SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG);
    com.codahale.metrics.Timer timer = metrics.timer(pipelineName + Worker.METRICS_SUFFIX);
    StringBuilder message = new StringBuilder(String.format(
        "%d docs processed. One minute rate: %.2f docs/sec. Mean pipeline latency: %.2f ms/doc.",
        timer.getCount(), timer.getOneMinuteRate(), timer.getSnapshot().getMean() / 1000000));
    String kafka = KafkaConsumerMetrics.workerSummary(pipelineName);
    if (!kafka.isEmpty()) {
      message.append(' ').append(kafka);
    }
    Meter queueFull = metrics.getMeters().get(pipelineName + HybridWorkerMessenger.INDEXER_QUEUE_FULL_SUFFIX);
    if (queueFull != null) {
      message.append(String.format(" Indexer queue: %d docs waited for space", queueFull.getCount()));
      if (includeRemainingCapacity) {
        message.append(String.format(", lowest remaining capacity %d", minRemainingCapacity(indexerQueues)));
      }
      message.append('.');
    }
    return message.toString();
  }

  /**
   * @return The lowest remaining capacity among the given queues (null entries are ignored), or 0 if there are none.
   */
  static int minRemainingCapacity(Collection<? extends BlockingQueue<?>> queues) {
    return queues.stream().filter(Objects::nonNull).mapToInt(BlockingQueue::remainingCapacity).min().orElse(0);
  }

  public int getNumWorkers() {
    return numWorkers;
  }

  /**
   * @return Whether every WorkerIndexer pair started by this pool still has both of its threads running.
   */
  public boolean isAlive() {
    if (workerIndexers.isEmpty()) {
      return false;
    }
    for (WorkerIndexer workerIndexer : workerIndexers) {
      if (!workerIndexer.isAlive()) {
        return false;
      }
    }
    return true;
  }

}
