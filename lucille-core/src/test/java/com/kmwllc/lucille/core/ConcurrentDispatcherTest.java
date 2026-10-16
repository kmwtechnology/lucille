package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
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
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Test;

/**
 * Tests {@link ConcurrentDispatcher} directly, with a fake {@link BatchProcessor}, for behavior that is awkward to
 * reach through a full Indexer: a send whose future is cancelled.
 */
public class ConcurrentDispatcherTest {

  private static final long TIMEOUT_MS = 10000;

  private ConcurrentDispatcher dispatcher;
  private final FakeProcessor processor = new FakeProcessor();

  @After
  public void tearDown() {
    processor.releaseAll();
    if (dispatcher != null) {
      dispatcher.close();
    }
  }

  @Test
  public void testCancelledSendIsCompletedAsAFailure() throws Exception {
    // If a send's future is cancelled, awaiting it throws CancellationException. The batch must still be completed (with
    // the failure as its outcome, so FAIL events are emitted) rather than the exception escaping and leaving the batch
    // unaccounted for. Nothing in the dispatcher cancels futures, so the test cancels the oldest one by reflection.
    processor.blockSendsFor("first");
    dispatcher = new ConcurrentDispatcher(2, "run", processor,
        docs -> docs.stream().map(d -> d.getString("destId")).collect(Collectors.toSet()), doc -> false);
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
    private final ConcurrentHashMap<String, CountDownLatch> latches = new ConcurrentHashMap<>();

    void blockSendsFor(String... docIds) {
      for (String docId : docIds) {
        latches.put(docId, new CountDownLatch(1));
      }
    }

    void releaseAll() {
      latches.values().forEach(CountDownLatch::countDown);
    }

    @Override
    public SendOutcome sendWithRetry(List<Document> batchedDocs) {
      String docId = batchedDocs.get(0).getId();
      started.add(docId);
      try {
        CountDownLatch latch = latches.get(docId);
        if (latch != null) {
          latch.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }
        return new SendOutcome(Set.of(), null, 0);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return new SendOutcome(null, e, 0);
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
