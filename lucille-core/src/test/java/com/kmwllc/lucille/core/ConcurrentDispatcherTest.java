package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Test;

/**
 * Tests {@link ConcurrentDispatcher} directly, with a fake {@link BatchProcessor}, for behavior that is awkward to
 * reach through a full Indexer: calls made into the dispatcher while it is itself waiting on an in-flight batch.
 */
public class ConcurrentDispatcherTest {

  private static final long TIMEOUT_MS = 10000;

  private ConcurrentDispatcher dispatcher;
  private Thread indexerThread;
  private final FakeProcessor processor = new FakeProcessor();

  @After
  public void tearDown() throws Exception {
    processor.releaseAll();
    if (indexerThread != null) {
      indexerThread.join(TIMEOUT_MS);
    }
    if (dispatcher != null) {
      dispatcher.close();
    }
  }

  @Test
  public void testReentrantDispatchWaitsForSameIdBatchBeingAwaited() throws Exception {
    // While the indexer thread waits on the oldest in-flight batch, keepAlive polls the consumer, and a rebalance there
    // runs the revocation hook, which flushes and drains: it calls dispatch() and drain() on this same thread. That
    // re-entrant dispatch must still see the batch being awaited. Here the re-entrant batch writes the same destination
    // id as the awaited one, so it must not be sent until the awaited batch has finished, and must complete after it.
    processor.blockSendsFor("first");
    AtomicInteger keepAliveCalls = new AtomicInteger();
    startDispatcher(keepAliveCalls, doc -> false, () -> {
      dispatcher.dispatch(List.of(doc("second", "sharedId")));
      dispatcher.drain();
    });

    awaitUntil(() -> keepAliveCalls.get() >= 1);
    Thread.sleep(300);
    assertFalse("second must wait: it writes the same id as the first, which is still in flight",
        processor.started.contains("second"));

    processor.release("first");
    indexerThread.join(TIMEOUT_MS);
    assertEquals(List.of("first", "second"), processor.completed);
    assertEquals("the two same-id batches must never overlap", 1, processor.peakConcurrent.get());
  }

  @Test
  public void testReentrantBarrierBatchWaitsForBatchBeingAwaited() throws Exception {
    // Same re-entrancy, but the re-entrant batch is a barrier (a delete-by-query), which must run with nothing else in
    // flight, including the batch being awaited, even though it shares no id with it.
    processor.blockSendsFor("first");
    AtomicInteger keepAliveCalls = new AtomicInteger();
    startDispatcher(keepAliveCalls, doc -> doc.getId().equals("barrier"), () -> {
      dispatcher.dispatch(List.of(doc("barrier", "otherId")));
      dispatcher.drain();
    });

    awaitUntil(() -> keepAliveCalls.get() >= 1);
    Thread.sleep(300);
    assertFalse("the barrier must wait for the in-flight first batch", processor.started.contains("barrier"));

    processor.release("first");
    indexerThread.join(TIMEOUT_MS);
    assertEquals(List.of("first", "barrier"), processor.completed);
    assertEquals("the barrier must run alone", 1, processor.peakConcurrent.get());
  }

  @Test
  public void testCancelledSendIsCompletedAsAFailure() throws Exception {
    // If a send's future is cancelled, awaiting it throws CancellationException. The batch must still be completed (with
    // the failure as its outcome, so FAIL events are emitted) rather than the exception escaping and leaving the batch
    // unaccounted for. Nothing in the dispatcher cancels futures, so the test cancels the oldest one by reflection.
    processor.blockSendsFor("first");
    dispatcher = new ConcurrentDispatcher(2, "run", processor,
        docs -> docs.stream().map(d -> d.getString("destId")).collect(Collectors.toSet()), doc -> false, () -> {});
    dispatcher.dispatch(List.of(doc("first", "a")));
    awaitUntil(() -> processor.started.contains("first"));

    Field inFlight = ConcurrentDispatcher.class.getDeclaredField("inFlightBatches");
    inFlight.setAccessible(true);
    Object head = ((Deque<?>) inFlight.get(dispatcher)).peekFirst();
    Method outcome = head.getClass().getDeclaredMethod("outcome");
    outcome.setAccessible(true);
    ((Future<?>) outcome.invoke(head)).cancel(true);

    dispatcher.drain();

    assertEquals(List.of("first"), processor.completed);
    assertTrue("the outcome must carry the cancellation", processor.errors.get("first") instanceof CancellationException);
  }

  // Starts the dispatcher and an indexer thread that dispatches "first" (destination id "sharedId") and then drains,
  // so the thread waits on "first" and the dispatcher's keepAlive fires. On its first call, keepAlive runs reentrant,
  // standing in for the revocation hook that a rebalance during the consumer poll would trigger. Later calls (including
  // those during the re-entrant drain) do nothing, as Indexer suppresses keepAlive while draining for a revocation.
  private void startDispatcher(AtomicInteger keepAliveCalls, Predicate<Document> isBarrier, Runnable reentrant) {
    boolean[] hookRan = {false};
    dispatcher = new ConcurrentDispatcher(3, "run", processor,
        docs -> docs.stream().map(d -> d.getString("destId")).collect(Collectors.toSet()),
        isBarrier,
        () -> {
          keepAliveCalls.incrementAndGet();
          if (!hookRan[0]) {
            hookRan[0] = true;
            reentrant.run();
          }
        });
    indexerThread = new Thread(() -> {
      dispatcher.dispatch(List.of(doc("first", "sharedId")));
      dispatcher.drain();
    });
    indexerThread.start();
  }

  private static Document doc(String docId, String destId) {
    Document doc = Document.create(docId);
    doc.setField("destId", destId);
    return doc;
  }

  private static void awaitUntil(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.currentTimeMillis() + TIMEOUT_MS;
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("Condition not met within " + TIMEOUT_MS + " ms");
      }
      Thread.sleep(5);
    }
  }

  /** A processor whose sends block until released, recording the order sends start and batches complete. */
  private static class FakeProcessor implements BatchProcessor {

    final List<String> started = Collections.synchronizedList(new ArrayList<>());
    final List<String> completed = Collections.synchronizedList(new ArrayList<>());
    final ConcurrentHashMap<String, Throwable> errors = new ConcurrentHashMap<>();
    final AtomicInteger peakConcurrent = new AtomicInteger();
    private final AtomicInteger concurrent = new AtomicInteger();
    private final ConcurrentHashMap<String, CountDownLatch> latches = new ConcurrentHashMap<>();

    void blockSendsFor(String... docIds) {
      for (String docId : docIds) {
        latches.put(docId, new CountDownLatch(1));
      }
    }

    void release(String docId) {
      latches.get(docId).countDown();
    }

    void releaseAll() {
      latches.values().forEach(CountDownLatch::countDown);
    }

    @Override
    public SendOutcome sendWithRetry(List<Document> batchedDocs) {
      String docId = batchedDocs.get(0).getId();
      started.add(docId);
      peakConcurrent.accumulateAndGet(concurrent.incrementAndGet(), Math::max);
      try {
        CountDownLatch latch = latches.get(docId);
        if (latch != null) {
          latch.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }
        return new SendOutcome(Set.of(), null, 0);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return new SendOutcome(null, e, 0);
      } finally {
        concurrent.decrementAndGet();
      }
    }

    @Override
    public void completeBatch(List<Document> batchedDocs, SendOutcome outcome) {
      completed.add(batchedDocs.get(0).getId());
      if (outcome.error() != null) {
        errors.put(batchedDocs.get(0).getId(), outcome.error());
      }
    }
  }
}
