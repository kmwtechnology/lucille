package com.kmwllc.lucille.core;

import com.kmwllc.lucille.message.WorkerMessageManager;
import com.kmwllc.lucille.message.WorkerMessageManagerFactory;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class WorkerPool {

  public static final int DEFAULT_POOL_SIZE = 1;

  private static final Logger log = LoggerFactory.getLogger(WorkerPool.class);

  private final List<WorkerThread> threads = new ArrayList();

  private final Config config;
  private final String pipelineName;
  private Integer numWorkers = null;
  private WorkerMessageManagerFactory workerMessageManagerFactory;
  private boolean started = false;

  public WorkerPool(Config config, String pipelineName, WorkerMessageManagerFactory factory) {
    this.config = config;
    this.pipelineName = pipelineName;
    this.workerMessageManagerFactory = factory;
    try {
       this.numWorkers = Pipeline.getIntProperty(config, pipelineName, "threads");
    } catch (PipelineException e) {
      log.error("Error reading pipeline config", e);
    }
    if (this.numWorkers==null) {
      this.numWorkers = config.hasPath("worker.threads") ? config.getInt("worker.threads") : DEFAULT_POOL_SIZE;
    }
  }

  public void start() throws Exception {
    if (started) {
      throw new IllegalStateException("WorkerPool can be started at most once");
    }
    started = true;
    log.info("Starting " + numWorkers + " worker threads");
    for (int i=0; i<numWorkers; i++) {
      WorkerMessageManager manager = workerMessageManagerFactory.create();
      threads.add(Worker.startThread(config,manager,pipelineName));
    }
  }

  public void stop() {
    log.info("Stopping " + threads.size() + " worker threads");
    for (WorkerThread workerThread : threads) {
      workerThread.terminate();
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
