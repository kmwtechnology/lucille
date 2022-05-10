package com.kmwllc.lucille.core;

import com.typesafe.config.Config;
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
  private Timer timer;
  private static final Logger log = LoggerFactory.getLogger(Worker.class);

  public static final String HEARTBEAT_LOG_NAME = "com.kmwllc.lucille.core.Heartbeat";
  private static final Logger heartbeatLog = LoggerFactory.getLogger(HEARTBEAT_LOG_NAME);

  private boolean enableHeartbeat;
  private int period;
  private int maxProcessingSecs;
  private boolean exitOnTimeout;

  public WorkerThread(Worker worker, Config config) {
    this.worker = worker;
    this.enableHeartbeat = config.hasPath("worker.heartbeat") ? config.getBoolean("worker.heartbeat") : false;
    this.period = config.hasPath("worker.period") ? config.getInt("worker.period") : 1000;
    // maxProcessingSecs default should be at least 10 minutes
    this.maxProcessingSecs = config.hasPath("worker.maxProcessingSecs") ? config.getInt("worker.maxProcessingSecs") : 10 * 60 * 1000;
    this.exitOnTimeout = config.hasPath("worker.exitOnTimeout") ? config.getBoolean("worker.exitOnTimeout") : false;

  }

  @Override
  public void run() {
    timer = spawnWatcher(worker, maxProcessingSecs);
    worker.run();
  }

  public void terminate() {
    timer.cancel();
    worker.terminate();
  }

  public void logMetrics() {
    worker.logMetrics();
  }

  public Timer spawnWatcher(Worker worker, int maxProcessingSecs) {

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

    Timer timer = new Timer("Timer");
    long delay = 1000L;
    timer.scheduleAtFixedRate(watcher, delay, period);
    return timer;
  }
}
