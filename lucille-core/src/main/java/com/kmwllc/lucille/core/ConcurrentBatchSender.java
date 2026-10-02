package com.kmwllc.lucille.core;

import com.kmwllc.lucille.core.Indexer.SendOutcome;
import com.kmwllc.lucille.util.ThreadNameUtils;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Sends an Indexer's batches on a pool of threads when indexer.maxConcurrentBatches is greater than 1. Only the send runs
 * on the pool; every method here is called on the indexer thread, which also completes batches, in dispatch order. See
 * "Concurrent sends" in the {@link Indexer} javadoc for the rules.
 */
class ConcurrentBatchSender {

  private static final Logger log = LoggerFactory.getLogger(ConcurrentBatchSender.class);
  private static final long SHUTDOWN_TIMEOUT_MS = 10000;
  private static final AtomicInteger INSTANCES = new AtomicInteger();

  private final int maxConcurrentBatches;
  private final Function<List<Document>, SendOutcome> send;
  private final BiConsumer<List<Document>, SendOutcome> complete;
  private final Function<List<Document>, Set<String>> destinationIds;
  private final Predicate<Document> isBarrier;
  private final ExecutorService pool;

  // Batches dispatched to the pool, oldest first.
  private final Deque<InFlightBatch> inFlight = new ArrayDeque<>();
  // Destination IDs of every batch in inFlight. Disjoint across batches by construction.
  private final Set<String> inFlightIds = new HashSet<>();

  private record InFlightBatch(List<Document> docs, Set<String> ids, Future<SendOutcome> outcome) {}

  /**
   * @param send sends a batch; runs on the pool and must not throw.
   * @param complete completes a sent batch; runs on the indexer thread.
   * @param destinationIds the IDs a batch writes at the destination; batches sharing one are never in flight together.
   * @param isBarrier whether a document (a delete-by-query) requires its batch to be the only one in flight.
   */
  ConcurrentBatchSender(int maxConcurrentBatches, String runId, Function<List<Document>, SendOutcome> send,
      BiConsumer<List<Document>, SendOutcome> complete, Function<List<Document>, Set<String>> destinationIds,
      Predicate<Document> isBarrier) {
    this.maxConcurrentBatches = maxConcurrentBatches;
    this.send = send;
    this.complete = complete;
    this.destinationIds = destinationIds;
    this.isBarrier = isBarrier;
    // Thread names are Lucille-<runId>-IndexerSend-<instance>-<n>; the instance number keeps them unique when several
    // indexers share a run. ThreadPoolExecutor starts threads lazily, so an Indexer that is never run holds none.
    int instance = INSTANCES.incrementAndGet();
    AtomicInteger count = new AtomicInteger();
    this.pool = Executors.newFixedThreadPool(maxConcurrentBatches, task -> {
      Thread thread = new Thread(task,
          ThreadNameUtils.createName("IndexerSend-" + instance + "-" + count.incrementAndGet(), runId));
      thread.setDaemon(true);
      return thread;
    });
  }

  /** Submits the batch once it may run alongside those in flight, completing the oldest in-flight batches until it can. */
  void dispatch(List<Document> batchedDocs) {
    Set<String> ids = destinationIds.apply(batchedDocs);
    boolean barrier = batchedDocs.stream().anyMatch(isBarrier);

    while (!inFlight.isEmpty() && (barrier || inFlight.size() >= maxConcurrentBatches || overlapsInFlight(ids))) {
      completeOldest();
    }

    Map<String, String> mdc = MDC.getCopyOfContextMap();
    Future<SendOutcome> outcome = pool.submit(() -> {
      if (mdc != null) {
        MDC.setContextMap(mdc);
      }
      try {
        return send.apply(batchedDocs);
      } finally {
        MDC.clear();
      }
    });
    inFlight.addLast(new InFlightBatch(batchedDocs, ids, outcome));
    inFlightIds.addAll(ids);

    if (barrier) {
      completeAll();
    }
  }

  /** Completes in-flight batches whose sends have finished, stopping at the first that hasn't, to keep dispatch order. */
  void completeFinished() {
    while (!inFlight.isEmpty() && inFlight.peekFirst().outcome().isDone()) {
      completeOldest();
    }
  }

  void completeAll() {
    while (!inFlight.isEmpty()) {
      completeOldest();
    }
  }

  /** Stops the pool. Sends still in flight (only when the indexer thread is exiting on an Error) are interrupted. */
  void shutdown() {
    if (inFlight.isEmpty()) {
      pool.shutdown();
    } else {
      log.warn("Shutting down indexer with {} batches still in flight.", inFlight.size());
      pool.shutdownNow();
    }
    try {
      if (!pool.awaitTermination(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        log.warn("Indexer send pool did not terminate within {} ms.", SHUTDOWN_TIMEOUT_MS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private boolean overlapsInFlight(Set<String> ids) {
    for (String id : ids) {
      if (inFlightIds.contains(id)) {
        return true;
      }
    }
    return false;
  }

  // Waits for the oldest in-flight batch's send to finish and completes it.
  //
  // The batch is completed with the interrupt status clear, because the messenger calls that complete it may block
  // (a queue put) and would otherwise throw at once, losing the batch's completion. Any interrupt, whether it arrived
  // before or during the wait, is restored afterwards so the next poll sees it.
  private void completeOldest() {
    InFlightBatch oldest = inFlight.peekFirst();
    // A send that has already finished never checks the flag, so clear it here rather than relying on the wait.
    boolean interrupted = Thread.interrupted();
    try {
      SendOutcome outcome = awaitOutcome(oldest.outcome());
      interrupted |= Thread.interrupted();
      inFlight.removeFirst();
      inFlightIds.removeAll(oldest.ids());
      complete.accept(oldest.docs(), outcome);
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  // Waits without giving up on interruption: the batch has been sent (or is being sent) and must be accounted for. An
  // interrupt that arrives during the wait is restored before returning.
  private static SendOutcome awaitOutcome(Future<SendOutcome> future) {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          return future.get();
        } catch (InterruptedException e) {
          interrupted = true;
        } catch (ExecutionException e) {
          // send() captures its own Throwables, so this only happens if the task wrapper itself failed.
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
