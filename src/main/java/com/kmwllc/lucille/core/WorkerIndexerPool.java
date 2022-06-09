package com.kmwllc.lucille.core;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.kmwllc.lucille.util.LogUtils;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class WorkerIndexerPool {

  public static final int DEFAULT_POOL_SIZE = 1;

  private static final Logger log = LoggerFactory.getLogger(WorkerPool.class);

  private final List<WorkerIndexer> workerIndexers = new ArrayList();

  private final Config config;
  private final String pipelineName;
  private Integer numWorkers = null;
  private boolean started = false;
  private final int logSeconds;
  private final Timer logTimer = new Timer();
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
    if (this.numWorkers==null) {
      this.numWorkers = config.hasPath("worker.threads") ? config.getInt("worker.threads") : DEFAULT_POOL_SIZE;
    }
    this.logSeconds = ConfigUtils.getOrDefault(config, "log.seconds", LogUtils.DEFAULT_LOG_SECONDS);
    this.idSet = idSet;
  }

  public void start() throws Exception {
    if (started) {
      throw new IllegalStateException("WorkerIndexerPool can be started at most once");
    }
    started = true;
    log.info("Starting " + numWorkers + " WorkerIndexer thread pairs for pipeline " + pipelineName);
    for (int i=0; i<numWorkers; i++) {
      WorkerIndexer workerIndexer = new WorkerIndexer();
      workerIndexer.start(config, pipelineName, bypassSearchEngine, idSet);
      workerIndexers.add(workerIndexer);
    }
    // Timer to log a status message every minute
    logTimer.schedule(new TimerTask() {
      private final MetricRegistry metrics = SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG);
      private final com.codahale.metrics.Timer timer = metrics.timer(pipelineName + Worker.METRICS_SUFFIX);
      @Override
      public void run() {
        log.info(String.format("%d docs processed. One minute rate: %.2f docs/sec. Mean pipeline latency: %.2f ms/doc.",
          timer.getCount(), timer.getOneMinuteRate(), timer.getSnapshot().getMean()/1000000));
      }
    }, logSeconds*1000, logSeconds*1000);

  }

  public void stop() throws Exception {
    log.debug("Stopping " + workerIndexers.size() + " worker threads");
    logTimer.cancel();
    for (WorkerIndexer workerIndexer : workerIndexers) {
      // stop each WorkerIndexer, one at a time;
      // make sure the indexer is stopped before the worker, so that the
      // worker has a chance to commit any final offsets that the indexer
      // added to the offset queue right before it stopped
      workerIndexer.stop();
    }
    // tell one of the threads to log its metrics;
    // the output should be the same for any thread;
    // all threads get their metrics via a shared registry using the same naming scheme,
    // so the metrics are collected across all the threads
    if (workerIndexers.size()>0) {
      workerIndexers.get(0).getWorker().logMetrics();
    }
  }

  public int getNumWorkers() {
    return numWorkers;
  }

}
