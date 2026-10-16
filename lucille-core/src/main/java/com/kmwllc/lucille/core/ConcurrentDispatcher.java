package com.kmwllc.lucille.core;

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
import java.util.function.Function;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * A {@link BatchDispatcher} that sends batches on a pool of threads when indexer.maxConcurrentBatches is greater than 1,
 * so that several sends can be in flight at once. A batch waits before being dispatched if the window is full, if it
 * shares a destination id with a batch still in flight, or if it is a delete-by-query barrier (which runs alone).
 * Batches are completed in the order they were dispatched.
 *
 * <p> <b>Threading.</b> Only the send ({@code processor.sendWithRetry}) runs on the pool. Every other method —
 * {@link #dispatch}, {@link #completeFinishedBatches}, {@link #drain}, and the completion they drive — is called on the
 * single indexer thread. The in-flight state ({@code inFlightBatches}, {@code inFlightIds}) is therefore confined to
 * that thread and needs no synchronization; the {@link Future} returned by the pool provides the happens-before edge
 * for each send's result. Nothing here calls back into the messenger while waiting, so these methods are never
 * re-entered.
 *
 * <p> <b>Waiting.</b> While {@link #dispatch} waits for room, the indexer thread does not poll its messenger. The wait
 * is bounded by the sends already in flight, which run in parallel and all started before it: about one send for a
 * full window or a shared destination id, and about two for a barrier (the in-flight sends, then its own). For a Kafka
 * messenger, kafka.maxPollIntervalSecs (also Kafka's rebalance timeout) must exceed that, and also the time from the
 * start of a rebalance to rejoining: the wait in progress plus the flush-and-drain the revocation hook runs at the
 * next poll, about three sends. Retries count toward a send.
 */
class ConcurrentDispatcher implements BatchDispatcher {

  private static final Logger log = LoggerFactory.getLogger(ConcurrentDispatcher.class);
  private static final long SHUTDOWN_TIMEOUT_MS = 10000;

  // Counts ConcurrentDispatcher instances created in this JVM, giving each a unique id. The id goes into this pool's
  // thread names so that indexers running at the same time (several concurrent runs can share a runId) have
  // distinguishable send threads. JVM-global and monotonic by design; never reset.
  private static final AtomicInteger DISPATCHER_INSTANCE_COUNT = new AtomicInteger();

  private final int maxConcurrentBatches;
  private final BatchProcessor processor;
  private final Function<List<Document>, Set<String>> destinationIdExtractor;
  // Whether a document forces its batch to run alone (a delete-by-query, which can match documents in any batch).
  private final Predicate<Document> isBarrierDocument;
  private final ExecutorService pool;

  // Batches dispatched to the pool, oldest first. Only touched by the indexer thread.
  private final Deque<InFlightBatch> inFlightBatches = new ArrayDeque<>();
  // The destination ids of every batch in inFlightBatches, unioned. Disjoint across batches by construction, since a
  // batch is not dispatched while it shares an id with one in flight. Only touched by the indexer thread.
  private final Set<String> inFlightIds = new HashSet<>();

  private static final class InFlightBatch {
    private final List<Document> docs;
    private final Set<String> ids;
    private final Future<SendOutcome> outcome;

    InFlightBatch(List<Document> docs, Set<String> ids, Future<SendOutcome> outcome) {
      this.docs = docs;
      this.ids = ids;
      this.outcome = outcome;
    }

    List<Document> docs() {
      return docs;
    }

    Set<String> ids() {
      return ids;
    }

    Future<SendOutcome> outcome() {
      return outcome;
    }
  }

  ConcurrentDispatcher(int maxConcurrentBatches, String runId,
      BatchProcessor processor,
      Function<List<Document>, Set<String>> destinationIdExtractor,
      Predicate<Document> isBarrierDocument) {
    this.maxConcurrentBatches = maxConcurrentBatches;
    this.processor = processor;
    this.destinationIdExtractor = destinationIdExtractor;
    this.isBarrierDocument = isBarrierDocument;
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
    Set<String> ids;
    boolean barrier;
    try {
      ids = destinationIdExtractor.apply(batchedDocs);
      // A barrier batch (one containing a delete-by-query, which can match documents in any batch) must run alone.
      barrier = batchedDocs.stream().anyMatch(isBarrierDocument);
    } catch (Exception e) {
      // The id extractor and barrier predicate inspect a document's fields (for example its idOverrideField), so a
      // malformed value can make them throw. They run on the indexer thread before the batch is submitted or recorded
      // in flight, so an escaping throw would kill the run loop with this batch's documents never completed, hanging
      // the run on events that never arrive. There is no way to prove these callbacks cannot throw at compile time
      // (unchecked exceptions are outside the type system), so the throw is contained here: the batch is failed the
      // same way a thrown send is, routed through completeBatch so FAIL events are emitted and the run proceeds. The
      // batches already in flight are drained first, so completion still happens in dispatch order.
      drain();
      processor.completeBatch(batchedDocs, new SendOutcome(null, e, 0));
      return;
    }
    // Wait until this batch may run alongside those in flight: a barrier waits for nothing to be in flight; otherwise
    // there must be a free slot and no in-flight batch may write any id this batch writes (so writes and deletes of the
    // same document stay in order).
    while (!inFlightBatches.isEmpty()
        && (barrier || inFlightBatches.size() >= maxConcurrentBatches || overlapsInFlight(ids))) {
      completeOldest();
    }
    Map<String, String> mdc = MDC.getCopyOfContextMap();
    // The task runs on a pool thread, so it must touch only processor.sendWithRetry and the per-thread MDC — never
    // the dispatcher's in-flight state (inFlightBatches, inFlightIds), which belongs to the indexer thread and is
    // unsynchronized.
    Future<SendOutcome> outcome = pool.submit(() -> {
      if (mdc != null) {
        MDC.setContextMap(mdc);
      }
      try {
        return processor.sendWithRetry(batchedDocs);
      } finally {
        MDC.clear();
      }
    });
    inFlightBatches.addLast(new InFlightBatch(batchedDocs, ids, outcome));
    inFlightIds.addAll(ids);
    // Keep a barrier alone: complete it before anything else is dispatched behind it.
    if (barrier) {
      drain();
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

  @Override
  public void drain() {
    while (!inFlightBatches.isEmpty()) {
      completeOldest();
    }
  }

  @Override
  public void close() {
    // On a normal stop, run() has drained every batch, so nothing is in flight and the pool is idle. If the indexer
    // thread is unwinding abnormally (an Error from a send or completion), batches may still be in flight; interrupt
    // their sends rather than waiting them out, since the run is already failing and their results would be discarded.
    if (inFlightBatches.isEmpty()) {
      pool.shutdown();
    } else {
      log.warn("Shutting down indexer send pool with {} batch(es) still in flight.", inFlightBatches.size());
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

  // For tests: whether the send pool has fully terminated (all send threads have stopped). True only after close().
  boolean sendPoolTerminated() {
    return pool.isTerminated();
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
    inFlightIds.removeAll(oldest.ids());
    processor.completeBatch(oldest.docs(), awaitOutcome(oldest.outcome()));
  }

  // Waits for a send to finish, even if the indexer thread is interrupted meanwhile.
  //
  // Nothing in Lucille interrupts the indexer thread (the Runner and WorkerIndexer stop it with terminate() and join()),
  // so this only matters when other code embeds Lucille and interrupts the thread itself, which is considered unlikely.
  // The batch has already been sent, so the wait is not abandoned; the interrupt is restored afterwards, so the next
  // poll sees it and the run loop exits. Completion then runs with the flag set, as it does after a synchronous send
  // when maxConcurrentBatches is 1; a messenger call that blocks interruptibly may fail, leaving the batch to be
  // re-delivered (Indexer.restoreInterrupt keeps that failure from swallowing the interrupt).
  private SendOutcome awaitOutcome(Future<SendOutcome> future) {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          return future.get();
        } catch (InterruptedException e) {
          // Throwing cleared the flag, so the next get() waits normally.
          interrupted = true;
        } catch (ExecutionException e) {
          // processor.sendWithRetry captures its own Throwables, so this only happens if the task wrapper itself failed.
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
