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
 * A {@link BatchDispatcher} that sends batches on a pool of threads when indexer.maxConcurrentBatches is greater than 1,
 * so that several sends can be in flight at once. A batch waits before being dispatched if the window is full, if it
 * shares a destination id with a batch still in flight, or if it is a delete-by-query barrier (which runs alone).
 * Batches are completed in the order they were dispatched.
 *
 * <p> <b>Threading.</b> Only the send ({@code batchSender}) runs on the pool. Every other method — {@link #dispatch},
 * {@link #completeFinishedBatches}, {@link #drain}, and the completion they drive — is called on the single indexer
 * thread. The in-flight state ({@code inFlightBatches}, {@code inFlightIds}) is therefore confined to that thread and
 * needs no synchronization; the {@link Future} returned by the pool provides the happens-before edge for each send's
 * result. Interrupt handling and shutdown draining are refined in later changes.
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
      Function<List<Document>, SendOutcome> batchSender,
      BiConsumer<List<Document>, SendOutcome> batchCompleter,
      Function<List<Document>, Set<String>> destinationIdExtractor,
      Predicate<Document> isBarrierDocument) {
    this.maxConcurrentBatches = maxConcurrentBatches;
    this.batchSender = batchSender;
    this.batchCompleter = batchCompleter;
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
    // The task runs on a pool thread, so it must touch only batchSender and the per-thread MDC — never the dispatcher's
    // in-flight state (inFlightBatches, inFlightIds), which belongs to the indexer thread and is unsynchronized.
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
    inFlightIds.removeAll(oldest.ids());
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
