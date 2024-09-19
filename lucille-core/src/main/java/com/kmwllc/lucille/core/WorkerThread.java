package com.kmwllc.lucille.core;

public class WorkerThread extends Thread {

  private final Worker worker;

  public WorkerThread(Worker worker, String name) {
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
