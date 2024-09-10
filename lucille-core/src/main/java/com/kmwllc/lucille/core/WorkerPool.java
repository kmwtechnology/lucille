package com.kmwllc.lucille.core;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.kmwllc.lucille.message.WorkerMessenger;
import com.kmwllc.lucille.message.WorkerMessengerFactory;
import com.kmwllc.lucille.util.LogUtils;
import com.kmwllc.lucille.util.ThreadNameUtils;
import com.typesafe.config.Config;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;

public class WorkerPool {

  public static final int DEFAULT_POOL_SIZE = 1;

  private static final Logger log = LoggerFactory.getLogger(WorkerPool.class);
  private static final long WATCHER_PERIOD_SECONDS = 5;

  private final List<WorkerThread> threads = new ArrayList<>();
  private ScheduledExecutorService watcherService;
  public static final String HEARTBEAT_LOG_NAME = "com.kmwllc.lucille.core.Heartbeat";
  private static final Logger heartbeatLog = LoggerFactory.getLogger(HEARTBEAT_LOG_NAME);

  private boolean enableHeartbeat;
  private int maxProcessingSecs;
  private boolean exitOnTimeout;

  private final Config config;
  private final String pipelineName;
  private Integer numWorkers = null;
  private WorkerMessengerFactory workerMessengerFactory;
  private boolean started = false;
  private final int logSeconds;
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
    this.enableHeartbeat = config.hasPath("worker.heartbeat") ? config.getBoolean("worker.heartbeat") : false;
    // maxProcessingSecs default should be at least 10 minutes
    this.maxProcessingSecs =
        config.hasPath("worker.maxProcessingSecs") ? config.getInt("worker.maxProcessingSecs") : 10 * 60 * 1000;
    this.exitOnTimeout = config.hasPath("worker.exitOnTimeout") ? config.getBoolean("worker.exitOnTimeout") : false;
  }

  public void start() throws Exception {
    if (started) {
      throw new IllegalStateException("WorkerPool can be started at most once");
    }
    started = true;
    log.info("Starting " + numWorkers + " worker threads for pipeline " + pipelineName);
    List<Worker> workers = new ArrayList<>();
    for (int i = 0; i < numWorkers; i++) {
      try {
        WorkerMessenger messenger = workerMessengerFactory.create();

        String name = ThreadNameUtils.createName("Worker-" + (i+1));
        // will throw exception if pipeline has errors
        Worker worker = new Worker(config, messenger, pipelineName, metricsPrefix);
        workers.add(worker);
        // start workerThread
        threads.add(Worker.startThread(worker, name));
        // start timer on last iteration to be within try catch block
        if (i == numWorkers - 1) {
          watcherService = startWatcher(workers, maxProcessingSecs);
        }
      } catch (Exception e) {
        log.error("Exception caught when starting Worker thread {}; aborting", i+1);
        try {
          stop();
        } catch (Exception e2) {
          log.error("Exception caught when attempting to stop Worker threads because of a startup problem", e2);
        }
        throw e;
      }
    }
  }

  public void stop() {
    log.debug("Stopping " + threads.size() + " worker threads");
    for (WorkerThread workerThread : threads) {
      workerThread.terminate();
    }
    // shutdown executorService gracefully
    if (watcherService != null) {
      stopWatcher(watcherService);
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

  private ScheduledExecutorService startWatcher(List<Worker> workers, int maxProcessingSecs) {
    TimerTask watcher = new TimerTask() {

      private final MetricRegistry metrics = SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG);
      private final com.codahale.metrics.Timer timer = metrics.timer(metricsPrefix + Worker.METRICS_SUFFIX);
      private Instant lastLogInstant = null;

      @Override
      public void run() {
        // log statistics about pipeline rate and latency
        if (lastLogInstant==null || Duration.between(lastLogInstant, Instant.now()).getSeconds() >= logSeconds) {
          lastLogInstant = Instant.now();
          log.info(String.format("%d docs processed. One minute rate: %.2f docs/sec. Mean pipeline latency: %.2f ms/doc.",
              timer.getCount(), timer.getOneMinuteRate(), timer.getSnapshot().getMean() / 1000000));

          // write message to heartbeat log
          if (enableHeartbeat) {
            heartbeatLog.info("Issuing heartbeat");
            if (heartbeatLog.isDebugEnabled()) {
              heartbeatLog.debug("Thread Dump:\n{}",
                  Arrays.toString(
                      ManagementFactory.getThreadMXBean().dumpAllThreads(true, true)));
            }
          }
        }

        // look for workers that might be "stuck"
        for (Worker worker : workers) {
          if (Duration.between(worker.getPreviousPollInstant().get(), Instant.now()).getSeconds() > maxProcessingSecs) {
            log.error("Worker has not polled in " + maxProcessingSecs + " seconds.");
            if (exitOnTimeout) {
              log.error("Shutting down because maximum allowed time between previous poll is exceeded.");
              System.exit(1);
            }
          }
        }
      }
    };

    // creating a thread factory for executor to use
    BasicThreadFactory factory = new BasicThreadFactory.Builder()
        .namingPattern(ThreadNameUtils.createName("WorkerWatcherExecutorService"))
        .daemon(true)
        .priority(Thread.NORM_PRIORITY)
        .build();

    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(factory);
    executor.scheduleAtFixedRate(watcher, 1, WATCHER_PERIOD_SECONDS, TimeUnit.SECONDS);
    return executor;
  }

  private void stopWatcher(ExecutorService executorService) {
    executorService.shutdown(); // disable new tasks from being submitted
    try {
      // wait a while for existing tasks to terminate
      if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
        executorService.shutdownNow(); // Cancel currently executing tasks
        // wait a while for tasks to respond to being cancelled
        if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
          log.error("Worker watcher did not terminate.");
        }
      }
    } catch (InterruptedException ex) {
      // cancel if current thread also interrupted
      executorService.shutdownNow();
      // preserve interrupt status
      Thread.currentThread().interrupt();
    }
  }

}
