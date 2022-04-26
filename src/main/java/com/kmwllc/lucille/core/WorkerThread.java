package com.kmwllc.lucille.core;

import org.apache.logging.log4j.core.util.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;

public class WorkerThread extends Thread {

  private final Worker worker;
  private Timer timer;
  private static final Logger log = LoggerFactory.getLogger(Worker.class);
  private static final Logger threadLog = LoggerFactory.getLogger("com.kmwllc.lucille.core.Heartbeat");

  public WorkerThread(Worker worker) {
    this.worker = worker;
  }

  @Override
  public void run() {
//    timer = spawnWatcher(worker, 50000);

    worker.run();
  }

  public void terminate() {
//    timer.cancel();
    worker.terminate();
  }

  public void logMetrics() {
    worker.logMetrics();
  }

  // updates: include watcher thread as an instance variable here
  // terminate watch thread with worker

  // move this entire method into workerthread, update it to return the watcherthread
  public static Timer spawnWatcher(Worker worker, int maxProcessingSecs) {

    TimerTask watcher = new TimerTask() {
      @Override
      public void run() {
        threadLog.info("Issuing heartbeat");
        threadLog.debug("Thread Dump:\n{}",
          Arrays.toString(
            ManagementFactory.getThreadMXBean().dumpAllThreads(true, true)));
        if (Duration.between(worker.getPreviousPollInstant().get(), Instant.now()).getSeconds() > maxProcessingSecs) {
          log.error("Shutting down because maximum allowed time between previous poll is exceeded.");
          System.exit(1);
        }
      }

    };

    Timer timer = new Timer("Timer");
    // delay should be fine hard coded, period should be configurable
    long delay = 1000L;
    long period = 1000L;
//    timer.scheduleAtFixedRate(watcher, delay, period);
    return timer;
  }

  // in run, start watcher first
  // in terminate, terminate watcher first

}
