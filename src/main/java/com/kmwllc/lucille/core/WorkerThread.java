package com.kmwllc.lucille.core;

public class WorkerThread extends Thread {

  private final Worker worker;

  public WorkerThread(Worker worker) {
    this.worker = worker;
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

  // updates: include watcher thread as an instance variable here
  // terminate watch thread with worker


  // in run, start watcher first
  // in terminate, terminate watcher first

}
