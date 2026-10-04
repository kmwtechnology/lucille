package com.kmwllc.lucille.core;

import com.kmwllc.lucille.core.Indexer.SendOutcome;
import com.kmwllc.lucille.util.ThreadNameUtils;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * A {@link BatchDispatcher} that sends batches on a pool of threads when indexer.maxConcurrentBatches is greater than 1,
 * so that several sends can be in flight at once. Only the send runs on the pool; every method here is called on the
 * indexer thread, which also completes batches, in the order they were dispatched.
 *
 * <p> This is built up over several changes. At this stage it supports concurrent sends with ordered completion and a
 * capacity limit (no more than maxConcurrentBatches batches in flight). The destination-ID and delete-by-query gates,
 * interrupt handling, and shutdown draining are added in later changes.
 */
class ConcurrentDispatcher implements BatchDispatcher {

  private static final Logger log = LoggerFactory.getLogger(ConcurrentDispatcher.class);
  private static final long SHUTDOWN_TIMEOUT_MS = 10000;

  // Counts ConcurrentDispatcher instances created in this JVM, giving each a unique id. The id goes into this pool's
  // thread names so that indexers running at the same time (several concurrent runs can share a runId) have
  // distinguishable send threads. JVM-global and monotonic by design; never reset.
  private static final AtomicInteger DISPATCHER_INSTANCE_COUNT = new AtomicInteger();

  private final int maxConcurrentBatches;
  private final Function<List<Document>, SendOutcome> batchSender;
  private final BiConsumer<List<Document>, SendOutcome> batchCompleter;
  private final ExecutorService pool;

  // Batches dispatched to the pool, oldest first. Only touched by the indexer thread.
  private final Deque<InFlightBatch> inFlightBatches = new ArrayDeque<>();

  private static final class InFlightBatch {
    private final List<Document> docs;
    private final Future<SendOutcome> outcome;

    InFlightBatch(List<Document> docs, Future<SendOutcome> outcome) {
      this.docs = docs;
      this.outcome = outcome;
    }

    List<Document> docs() {
      return docs;
    }

    Future<SendOutcome> outcome() {
      return outcome;
    }
  }

  ConcurrentDispatcher(int maxConcurrentBatches, String runId,
      Function<List<Document>, SendOutcome> batchSender,
      BiConsumer<List<Document>, SendOutcome> batchCompleter) {
    this.maxConcurrentBatches = maxConcurrentBatches;
    this.batchSender = batchSender;
    this.batchCompleter = batchCompleter;
    // Thread names are Lucille-<runId>-IndexerSend-<dispatcherInstanceId>-<sendThreadNum>: dispatcherInstanceId
    // distinguishes this pool from other concurrent dispatchers' pools, sendThreadNum distinguishes the send threads
    // within this pool.
    int dispatcherInstanceId = DISPATCHER_INSTANCE_COUNT.incrementAndGet();
    AtomicInteger sendThreadNum = new AtomicInteger();
    this.pool = Executors.newFixedThreadPool(maxConcurrentBatches, task -> {
      Thread thread = new Thread(task,
          ThreadNameUtils.createName("IndexerSend-" + dispatcherInstanceId + "-" + sendThreadNum.incrementAndGet(), runId));
      thread.setDaemon(true);
      return thread;
    });
  }

  @Override
  public void dispatch(List<Document> batchedDocs) {
    // Make room: while the window is full, complete the oldest in-flight batch.
    while (inFlightBatches.size() >= maxConcurrentBatches) {
      completeOldest();
    }

    Map<String, String> mdc = MDC.getCopyOfContextMap();
    Future<SendOutcome> outcome = pool.submit(() -> {
      if (mdc != null) {
        MDC.setContextMap(mdc);
      }
      try {
        return batchSender.apply(batchedDocs);
      } finally {
        MDC.clear();
      }
    });
    inFlightBatches.addLast(new InFlightBatch(batchedDocs, outcome));
  }

  @Override
  public void drain() {
    while (!inFlightBatches.isEmpty()) {
      completeOldest();
    }
  }

  @Override
  public void close() {
    pool.shutdown();
    try {
      if (!pool.awaitTermination(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        log.warn("Indexer send pool did not terminate within {} ms.", SHUTDOWN_TIMEOUT_MS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  // Completes in-flight batches whose sends have finished, stopping at the first that hasn't, so completion order
  // matches dispatch order.
  @Override
  public void completeFinishedBatches() {
    while (!inFlightBatches.isEmpty() && inFlightBatches.peekFirst().outcome().isDone()) {
      completeOldest();
    }
  }

  // Waits for the oldest in-flight batch's send to finish and completes it, on the indexer thread.
  private void completeOldest() {
    InFlightBatch oldest = inFlightBatches.removeFirst();
    batchCompleter.accept(oldest.docs(), awaitOutcome(oldest.outcome()));
  }

  // Waits for a send to finish. The batch has been sent (or is being sent) and must be accounted for, so an interrupt
  // during the wait is noted and restored afterwards rather than abandoning the batch.
  private static SendOutcome awaitOutcome(Future<SendOutcome> future) {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          return future.get();
        } catch (InterruptedException e) {
          interrupted = true;
        } catch (ExecutionException e) {
          // batchSender captures its own Throwables, so this only happens if the task wrapper itself failed.
          return new SendOutcome(null, e.getCause(), 0);
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
