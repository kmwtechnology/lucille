package com.kmwllc.lucille.core;

/**
 * The thread a {@link Worker} runs on. Records a failure that ends the worker so that a {@link WorkerPool} can tell an
 * abnormal exit from a requested one.
 */
public class WorkerThread extends Thread {

  private final Worker worker;

  private volatile Throwable failure;

  public WorkerThread(Worker worker, String name) {
    this.worker = worker;
    this.setName(name);
  }

  @Override
  public void run() {
    try {
      worker.run();
    } catch (Throwable t) {
      failure = t;
      throw t;
    }
  }

  public void terminate() {
    worker.terminate();
  }

  public void logMetrics() {
    worker.logMetrics();
  }

  Worker getWorker() {
    return worker;
  }

  /**
   * @return The Throwable that stopped the worker, or null if it has not failed (it may still be running, or have
   * stopped because it was asked to).
   */
  public Throwable getFailure() {
    return failure != null ? failure : worker.getFailure();
  }
}
