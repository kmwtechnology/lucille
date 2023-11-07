package com.kmwllc.lucille.core;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.kmwllc.lucille.message.WorkerMessenger;
import com.kmwllc.lucille.message.WorkerMessengerFactory;
import com.kmwllc.lucille.util.LogUtils;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class WorkerPool {

  public static final int DEFAULT_POOL_SIZE = 1;

  private static final Logger log = LoggerFactory.getLogger(WorkerPool.class);

  private final List<WorkerThread> threads = new ArrayList();

  private final Config config;
  private final String pipelineName;
  private Integer numWorkers = null;
  private WorkerMessengerFactory workerMessengerFactory;
  private boolean started = false;
  private final int logSeconds;
  private final Timer logTimer = new Timer();
  private final String metricsPrefix;

  public WorkerPool(Config config, String pipelineName, WorkerMessengerFactory factory, String metricsPrefix) {
    this.config = config;
    this.pipelineName = pipelineName;
    this.workerMessengerFactory = factory;
    this.metricsPrefix = metricsPrefix;
    try {
      this.numWorkers = Pipeline.getIntProperty(config, pipelineName, "threads");
    } catch (PipelineException e) {
      log.error("Error reading pipeline config", e);
    }
    if (this.numWorkers == null) {
      this.numWorkers = config.hasPath("worker.threads") ? config.getInt("worker.threads") : DEFAULT_POOL_SIZE;
    }
    this.logSeconds = ConfigUtils.getOrDefault(config, "log.seconds", LogUtils.DEFAULT_LOG_SECONDS);
  }

  public void start() throws Exception {
    if (started) {
      throw new IllegalStateException("WorkerPool can be started at most once");
    }
    started = true;
    log.info("Starting " + numWorkers + " worker threads for pipeline " + pipelineName);
    for (int i = 0; i < numWorkers; i++) {
      WorkerMessenger messenger = workerMessengerFactory.create();
      threads.add(Worker.startThread(config, messenger, pipelineName, metricsPrefix));
    }
    // Timer to log a status message every minute
    logTimer.schedule(new TimerTask() {
      private final MetricRegistry metrics = SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG);
      private final com.codahale.metrics.Timer timer = metrics.timer(metricsPrefix + Worker.METRICS_SUFFIX);

      @Override
      public void run() {
        log.info(String.format("%d docs processed. One minute rate: %.2f docs/sec. Mean pipeline latency: %.2f ms/doc.",
            timer.getCount(), timer.getOneMinuteRate(), timer.getSnapshot().getMean() / 1000000));
      }
    }, logSeconds * 1000, logSeconds * 1000);

  }

  public void stop() {
    log.debug("Stopping " + threads.size() + " worker threads");
    logTimer.cancel();
    for (WorkerThread workerThread : threads) {
      workerThread.terminate();
    }
    // tell one of the threads to log its metrics;
    // the output should be the same for any thread;
    // all threads get their metrics via a shared registry using the same naming scheme,
    // so the metrics are collected across all the threads
    if (threads.size() > 0) {
      threads.get(0).logMetrics();
    }
  }

  public void join() throws InterruptedException {
    for (WorkerThread workerThread : threads) {
      workerThread.join();
    }
  }

  public void join(long millis) throws InterruptedException {
    for (WorkerThread workerThread : threads) {
      workerThread.join(millis);
    }
  }

  public int getNumWorkers() {
    return numWorkers;
  }

}
