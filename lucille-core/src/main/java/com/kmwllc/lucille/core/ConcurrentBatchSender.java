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
 * Sends an Indexer's batches on a pool of K threads, where K is indexer.maxConcurrentBatches (greater than 1).
 *
 * <p> The model is a sliding window: keep up to K batches in flight. Before sending a batch, complete the oldest batch in
 * flight until there is room and no conflict. Only the send (sendToIndex, with retries) runs on the pool; batches are
 * completed on the indexer thread, in the order they were sent. Polling and every IndexerMessenger call stay on the
 * indexer thread.
 *
 * <p> Guarantees:
 * <ul>
 *   <li>At most K batches are in flight.</li>
 *   <li>Batches are completed in send order, so a messenger that commits its input on batch completion never commits
 *   past an unfinished batch. (With indexer.indexOverrideField, batches are kept per index and one index's batch can be
 *   flushed before another's holding earlier input; that is true of synchronous sends too.)</li>
 *   <li>A batch is not sent while it shares a destination ID with a batch in flight, so writes and deletes of the same
 *   document are applied in order. A document's destination IDs are its own (the idOverrideField value, or the document
 *   ID) and the document IDs of its children, recursively.</li>
 *   <li>A batch containing a delete-by-query document is sent only when nothing else is in flight, and is completed
 *   before anything is sent behind it.</li>
 *   <li>An interrupt does not abandon a batch in flight: it is held while the indexer thread waits for and completes the
 *   batch, then restored so the next poll sees it.</li>
 * </ul>
 *
 * <p> Why a sliding window rather than sending batches in groups of K and waiting for each group: a group waits for its
 * slowest bulk request, and in benchmarks that made generations 14-25% slower. The window refills a slot as soon as the
 * oldest batch completes.
 */
class ConcurrentBatchSender implements BatchSender {

  private static final Logger log = LoggerFactory.getLogger(ConcurrentBatchSender.class);
  private static final long SHUTDOWN_TIMEOUT_MS = 10000;
  private static final AtomicInteger INSTANCES = new AtomicInteger();

  private final int maxConcurrentBatches;
  private final Function<List<Document>, SendOutcome> send;
  private final BiConsumer<List<Document>, SendOutcome> complete;
  private final Function<List<Document>, Set<String>> destinationIds;
  private final Predicate<Document> isBarrier;
  private final ExecutorService pool;

  // Batches sent to the pool, oldest first.
  private final Deque<InFlightBatch> inFlight = new ArrayDeque<>();
  // Destination IDs of every batch in inFlight. Disjoint across batches by construction.
  private final Set<String> inFlightIds = new HashSet<>();

  private record InFlightBatch(List<Document> docs, Set<String> ids, Future<SendOutcome> outcome) {}

  /**
   * @param send sends a batch; runs on the pool and must not throw.
   * @param complete completes a sent batch; runs on the indexer thread, and must shield itself from interrupts.
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

  /** Submits the batch to the pool once it may run alongside those in flight, completing the oldest until it can. */
  @Override
  public void send(List<Document> batch) {
    Set<String> ids = destinationIds.apply(batch);
    boolean barrier = batch.stream().anyMatch(isBarrier);

    while (!inFlight.isEmpty() && (barrier || inFlight.size() >= maxConcurrentBatches || overlapsInFlight(ids))) {
      completeOldest();
    }

    Map<String, String> mdc = MDC.getCopyOfContextMap();
    Future<SendOutcome> outcome = pool.submit(() -> {
      if (mdc != null) {
        MDC.setContextMap(mdc);
      }
      try {
        return send.apply(batch);
      } finally {
        MDC.clear();
      }
    });
    inFlight.addLast(new InFlightBatch(batch, ids, outcome));
    inFlightIds.addAll(ids);

    if (barrier) {
      completeAll();
    }
  }

  /** Completes in-flight batches whose sends have finished, stopping at the first that hasn't, to keep send order. */
  @Override
  public void completeFinished() {
    while (!inFlight.isEmpty() && inFlight.peekFirst().outcome().isDone()) {
      completeOldest();
    }
  }

  @Override
  public void completeAll() {
    while (!inFlight.isEmpty()) {
      completeOldest();
    }
  }

  /** Stops the pool. Sends still in flight (only when the indexer thread is exiting on an Error) are interrupted. */
  @Override
  public void close() {
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

  // Waits for the oldest in-flight batch's send to finish and completes it. The completion callback (Indexer.completeBatch)
  // shields the messenger calls from any interrupt and restores it.
  private void completeOldest() {
    InFlightBatch oldest = inFlight.removeFirst();
    inFlightIds.removeAll(oldest.ids());
    complete.accept(oldest.docs(), awaitOutcome(oldest.outcome()));
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
