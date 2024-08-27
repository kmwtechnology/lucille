package com.kmwllc.lucille.core;


import com.kmwllc.lucille.util.ThreadNameUtils;
import com.typesafe.config.Config;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.solr.common.util.ExecutorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;

public class WorkerThread extends Thread {

  private final Worker worker;
  private ScheduledExecutorService executorService;
  private static final Logger log = LoggerFactory.getLogger(Worker.class);

  public static final String HEARTBEAT_LOG_NAME = "com.kmwllc.lucille.core.Heartbeat";
  private static final Logger heartbeatLog = LoggerFactory.getLogger(HEARTBEAT_LOG_NAME);

  private boolean enableHeartbeat;
  private int period;
  private int maxProcessingSecs;
  private boolean exitOnTimeout;
  private final String name;

  public WorkerThread(Worker worker, Config config, String name) {
    this.worker = worker;
    this.name = name;
    this.setName(name);
    this.enableHeartbeat = config.hasPath("worker.heartbeat") ? config.getBoolean("worker.heartbeat") : false;
    this.period = config.hasPath("worker.period") ? config.getInt("worker.period") : 1000;
    // maxProcessingSecs default should be at least 10 minutes
    this.maxProcessingSecs =
        config.hasPath("worker.maxProcessingSecs") ? config.getInt("worker.maxProcessingSecs") : 10 * 60 * 1000;
    this.exitOnTimeout = config.hasPath("worker.exitOnTimeout") ? config.getBoolean("worker.exitOnTimeout") : false;
  }

  @Override
  public void run() {
    if (worker.isTerminated()) {
      return;
    }
    executorService = startTimer(worker, maxProcessingSecs);
    log.info("started executorService");
    try {
      worker.run();
    } finally {
      if (executorService != null) {
        try {
          executorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          log.warn("interrupted while waiting for executor service to terminate");
        }
      }
    }
  }

  public void terminate() {
    /*
    if (executorService != null) {
      log.info("Terminating timer in thread {}", this.getName());
      shutdownAndAwaitTermination(executorService);
      log.info("Terminated timer in thread {}", this.getName());
    }
     */
    worker.terminate();
  }

  public void shutdownAndAwaitTermination(ExecutorService pool) {
    pool.shutdown(); // Disable new tasks from being submitted
    try {
      // Wait a while for existing tasks to terminate
      if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
        pool.shutdownNow(); // Cancel currently executing tasks
        // Wait a while for tasks to respond to being cancelled
        if (!pool.awaitTermination(60, TimeUnit.SECONDS))
          System.err.println("Pool did not terminate");
      }
    } catch (InterruptedException ex) {
      // (Re-)Cancel if current thread also interrupted
      pool.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
  }

  public void logMetrics() {
    worker.logMetrics();
  }

  private ScheduledExecutorService startTimer(Worker worker, int maxProcessingSecs) {

    TimerTask watcher = new TimerTask() {
      @Override
      public void run() {
        if (enableHeartbeat) {
          heartbeatLog.info("Issuing heartbeat");
          if (heartbeatLog.isDebugEnabled()) {
            heartbeatLog.debug("Thread Dump:\n{}",
                Arrays.toString(
                    ManagementFactory.getThreadMXBean().dumpAllThreads(true, true)));
          }
        }
        if (Duration.between(worker.getPreviousPollInstant().get(), Instant.now()).getSeconds() > maxProcessingSecs) {
          log.error("Worker has not polled in " + maxProcessingSecs + " seconds.");
          if (exitOnTimeout) {
            log.error("Shutting down because maximum allowed time between previous poll is exceeded.");
            System.exit(1);
          }
        }
      }
    };

    // creating a thread factory for executor to use
    BasicThreadFactory factory = new BasicThreadFactory.Builder()
        .namingPattern(name + "-" + "ExecutorService")
        .daemon(true)
        .priority(Thread.NORM_PRIORITY)
        .build();

    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(factory);
    long delay = 1000L;
    executor.scheduleAtFixedRate(watcher, delay, period, TimeUnit.MILLISECONDS);
    return executor;
  }
}
