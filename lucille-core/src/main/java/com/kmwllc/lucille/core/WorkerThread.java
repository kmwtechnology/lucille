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

  public WorkerThread(Worker worker, Config config, String name) {
    this.worker = worker;
    this.setName(name);
  }

  @Override
  public void run() {
    worker.run();
  }

  public void terminate() {
    worker.terminate();
  }

  public void logMetrics() {
    worker.logMetrics();
  }
}
