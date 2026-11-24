package com.kmwllc.lucille.core;

import com.kmwllc.lucille.indexer.IndexerFactory;
import com.kmwllc.lucille.message.HybridIndexerMessenger;
import com.kmwllc.lucille.message.HybridWorkerMessenger;
import com.kmwllc.lucille.message.LocalMessenger;
import com.kmwllc.lucille.util.ThreadNameUtils;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


/**
 * Provides a way to launch a Worker-Indexer pair, where:
 *  1) the Worker reads documents from a source kafka topic
 *  2) the Worker publishes documents to an in-memory queue
 *  3) the Indexer retrieves documents from an in-memory queue
 *  4) the Indexer reports each completed batch to the Worker's messenger
 *  5) the Worker commits the offsets of completed batches, never past an unfinished record
 *  6) callbacks are disabled
 *
 *  TODO: document why pairing of one worker with one indexer is necessary for hybrid mode
 */
public class WorkerIndexer {

  private static final Logger log = LoggerFactory.getLogger(WorkerIndexer.class);

  // how long stop() waits for the worker between clearing the queue
  private static final long WORKER_JOIN_INTERVAL_MS = 100;

  private Indexer indexer;
  private Thread indexerThread;
  private WorkerThread workerThread;
  private BlockingQueue<Document> pipelineDest;

  /**
   * Starts paired Worker and Indexer threads for the pipeline named by the first argument (or by
   * <code>worker.pipeline</code>). The process runs until it is told to stop (SIGINT or SIGTERM); if a thread stops on
   * its own, the rest are stopped and the process exits 1. The body of this entry point is
   * {@link Launch#workerIndexer}.
   */
  public static void main(String[] args) throws Exception {
    Launch.workerIndexer(args);
  }

  /**
   * @return Whether both the worker thread and the indexer thread are still running.
   */
  public boolean isAlive() {
    return workerThread != null && workerThread.isAlive() && indexerThread != null && indexerThread.isAlive();
  }


  public void start(Config config, String pipelineName, boolean bypassSearchEngine, Set<String> idSet) throws Exception {

    // TODO: make queue capacity configurable
    LinkedBlockingQueue<Document> pipelineDest =
        new LinkedBlockingQueue<>(LocalMessenger.DEFAULT_QUEUE_CAPACITY);

    start(config, pipelineName, pipelineDest, bypassSearchEngine, idSet);
  }

  // takes the queue the Worker and Indexer share, so tests can supply a queue they inspect
  void start(Config config, String pipelineName, BlockingQueue<Document> pipelineDest,
      boolean bypassSearchEngine,
      Set<String> idSet) throws Exception {

    log.info("Starting WorkerIndexer for pipeline: {}", pipelineName);
    this.pipelineDest = pipelineDest;

    HybridWorkerMessenger workerMessageManager =
        new HybridWorkerMessenger(config, pipelineName, pipelineDest);

    HybridIndexerMessenger indexerMessageManager =
        new HybridIndexerMessenger(config, pipelineDest, workerMessageManager, idSet, pipelineName);

    indexer = IndexerFactory.fromConfig(config, indexerMessageManager, bypassSearchEngine, pipelineName, null);

    if (!indexer.validateConnection()) {
      throw new IndexerException("Indexer could not connect");
    }

    indexerThread = new Thread(indexer);
    indexerThread.start();
    String name = ThreadNameUtils.createName("WorkerIndexer");
    // Since Kafka is in use, we do not provide (or even have for that matter) a runId to directly provide to Worker.
    Worker worker = new Worker(config, workerMessageManager, null, pipelineName, pipelineName);
    workerThread = Worker.startThread(worker, name);
  }

  /**
   * Stops the pair as {@link #stop(long)} does, waiting up to {@link Runner#DEFAULT_WORKER_INDEXER_JOIN_TIMEOUT}
   * milliseconds for each of the two threads.
   */
  public void stop() throws Exception {
    stop(Runner.DEFAULT_WORKER_INDEXER_JOIN_TIMEOUT);
  }

  /**
   * Stops the Indexer, then the Worker, waiting up to the given time for each, so the call returns within about twice
   * that time. A thread that has not stopped by then is left running and a warning is logged.
   *
   * <p>Documents still waiting in the in-memory queue when the Worker is stopped are discarded, which frees a Worker
   * that is blocked because the queue is full. They are not lost: the offsets of the records they came from are not
   * committed, so those records are redelivered. The same holds for a batch the Indexer was still sending when its
   * wait ran out.
   *
   * <p>Safe to call more than once.
   *
   * @param timeoutMs How long to wait for each thread to stop, in milliseconds.
   */
  public void stop(long timeoutMs) throws Exception {
    // it is important to terminate the indexer and wait for its thread to stop
    // before we terminate the worker. This allows the worker to process
    // the batches the indexer completes upon termination, and commit their offsets
    terminateIndexer();
    joinIndexer(deadlineAfter(timeoutMs));
    terminateWorker();
    joinWorker(deadlineAfter(timeoutMs));
  }

  // The parts of stop(), which WorkerIndexerPool runs for all of its pairs at once: every Indexer is told to stop before
  // any is waited for, and the same for the Workers, so the pairs share a deadline instead of each using its own.

  static long deadlineAfter(long timeoutMs) {
    return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
  }

  void terminateIndexer() {
    if (indexer != null) {
      indexer.terminate();
      log.info("Indexer shutting down");
    }
  }

  // Waits for the Indexer thread until the deadline (a System.nanoTime value). If it is still running then, such as
  // during a slow final request, the stop goes on without it: the batch it is sending is not committed.
  void joinIndexer(long deadlineNanos) {
    if (indexerThread == null) {
      return;
    }
    try {
      long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
      if (remainingMs > 0) {
        indexerThread.join(remainingMs);
      }
    } catch (InterruptedException e) {
      log.error("Interrupted", e);
      Thread.currentThread().interrupt();
    }
    if (indexerThread.isAlive()) {
      log.warn("Indexer thread {} did not stop in time; stopping the worker without waiting for it. The batch it is "
          + "sending will not be committed, so its documents will be redelivered.", indexerThread.getName());
    }
  }

  void terminateWorker() {
    if (workerThread != null) {
      workerThread.terminate();
    }
  }

  // Waits for the Worker thread until the deadline (a System.nanoTime value), emptying the queue as it waits. Once the
  // Indexer has stopped nothing else takes from the queue, so a Worker that found it full would otherwise wait forever.
  // A document removed here was never indexed, so its record never finishes and its offset is not committed. A Worker
  // freed this way may queue a few more documents before it sees that it was terminated; the next pass removes them.
  void joinWorker(long deadlineNanos) {
    if (workerThread == null) {
      return;
    }
    try {
      do {
        if (pipelineDest != null) {
          pipelineDest.clear();
        }
        workerThread.join(WORKER_JOIN_INTERVAL_MS);
      } while (workerThread.isAlive() && System.nanoTime() < deadlineNanos);
    } catch (InterruptedException e) {
      log.error("Interrupted", e);
      Thread.currentThread().interrupt();
    }
    if (workerThread.isAlive()) {
      log.warn("Worker thread {} did not stop in time.", workerThread.getName());
    }
  }

  public Indexer getIndexer() {
    return indexer;
  }

  public WorkerThread getWorker() {
    return workerThread;
  }

  /**
   * @return The queue the worker puts processed documents on for the indexer, or null if this pair has not been started.
   */
  BlockingQueue<Document> getIndexerQueue() {
    return pipelineDest;
  }


}
