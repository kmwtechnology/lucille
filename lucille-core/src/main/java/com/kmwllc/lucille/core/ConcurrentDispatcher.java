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
import java.util.concurrent.TimeoutException;
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
 * for each send's result.
 */
class ConcurrentDispatcher implements BatchDispatcher {

  private static final Logger log = LoggerFactory.getLogger(ConcurrentDispatcher.class);
  private static final long SHUTDOWN_TIMEOUT_MS = 10000;
  // How often keepAlive is invoked while the indexer thread waits for an in-flight batch to finish.
  static final long KEEP_ALIVE_INTERVAL_MS = 1000;

  // Counts ConcurrentDispatcher instances created in this JVM, giving each a unique id. The id goes into this pool's
  // thread names so that indexers running at the same time (several concurrent runs can share a runId) have
  // distinguishable send threads. JVM-global and monotonic by design; never reset.
  private static final AtomicInteger DISPATCHER_INSTANCE_COUNT = new AtomicInteger();

  private final int maxConcurrentBatches;
  private final BatchProcessor processor;
  private final Function<List<Document>, Set<String>> destinationIdExtractor;
  // Whether a document forces its batch to run alone (a delete-by-query, which can match documents in any batch).
  private final Predicate<Document> isBarrierDocument;
  // Called periodically while the indexer thread waits for an in-flight batch, so a messenger that must contact a
  // source to stay alive (such as a Kafka consumer group member) can do so. Must not throw.
  private final Runnable keepAlive;
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
      Predicate<Document> isBarrierDocument,
      Runnable keepAlive) {
    this.maxConcurrentBatches = maxConcurrentBatches;
    this.processor = processor;
    this.destinationIdExtractor = destinationIdExtractor;
    this.isBarrierDocument = isBarrierDocument;
    this.keepAlive = keepAlive;
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
    Set<String> ids = destinationIdExtractor.apply(batchedDocs);
    // A barrier batch (one containing a delete-by-query, which can match documents in any batch) must run alone.
    boolean barrier = batchedDocs.stream().anyMatch(isBarrierDocument);
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
  //
  // Completion runs with the interrupt flag clear, because the completing messenger calls may themselves make
  // interruptible blocking calls (e.g. a queue put) that would otherwise throw at once and lose the batch's completion.
  // Any interrupt — whether it was already pending or arrived during the wait — is restored afterwards, so the next
  // poll observes it and the run loop exits. See the note on interrupts in awaitOutcome.
  private void completeOldest() {
    InFlightBatch oldest = inFlightBatches.removeFirst();
    inFlightIds.removeAll(oldest.ids());
    // Clear and remember any already-pending interrupt, so completion below runs with the flag clear.
    boolean interrupted = Thread.interrupted();
    try {
      AwaitedOutcome awaited = awaitOutcome(oldest.outcome());
      interrupted |= awaited.interrupted();
      processor.completeBatch(oldest.docs(), awaited.outcome());
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  // Waits for a send to finish, calling keepAlive every KEEP_ALIVE_INTERVAL_MS while it is unfinished.
  //
  // On interrupts: an interrupt arriving here is rare. Normal shutdown does not interrupt the indexer thread — the
  // Runner and WorkerIndexer stop the indexer with terminate() (which sets a flag) followed by join(), letting the run
  // loop exit on its own. An interrupt reaches this wait only in edge cases: a forced or timed-out shutdown escalating
  // to Thread.interrupt() or an executor shutdownNow() while a send happens to be slow, or JVM/container termination.
  //
  // Rare as it is, it must be handled rather than propagated, because the batch has already been sent (or is being
  // sent) and must still be accounted for: its completion emits the batch's events and (for a commit-on-completion
  // messenger) commits its offsets. Abandoning it on interrupt would lose those — a hung run waiting on events that
  // never arrive, or uncommitted offsets. So an interrupt is reported (not re-thrown) and the wait continues until the
  // send finishes; completeOldest clears the flag across completion and restores it afterwards.
  //
  // Returns with the interrupt flag left as the caller set it; the returned AwaitedOutcome says whether an interrupt
  // arrived during the wait.
  private AwaitedOutcome awaitOutcome(Future<SendOutcome> future) {
    boolean interrupted = false;
    while (true) {
      try {
        return new AwaitedOutcome(future.get(KEEP_ALIVE_INTERVAL_MS, TimeUnit.MILLISECONDS), interrupted);
      } catch (InterruptedException e) {
        interrupted = true;
      } catch (TimeoutException e) {
        keepAlive.run();
      } catch (ExecutionException e) {
        // processor.sendWithRetry captures its own Throwables, so this only happens if the task wrapper itself failed.
        return new AwaitedOutcome(new SendOutcome(null, e.getCause(), 0), interrupted);
      }
    }
  }

  // The result of awaiting a send: its outcome, and whether an interrupt arrived during the wait.
  private static final class AwaitedOutcome {
    private final SendOutcome outcome;
    private final boolean interrupted;

    AwaitedOutcome(SendOutcome outcome, boolean interrupted) {
      this.outcome = outcome;
      this.interrupted = interrupted;
    }

    SendOutcome outcome() {
      return outcome;
    }

    boolean interrupted() {
      return interrupted;
    }
  }
}
