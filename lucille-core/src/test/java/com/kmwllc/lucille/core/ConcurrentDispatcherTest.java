package com.kmwllc.lucille.core;

import static com.kmwllc.lucille.core.FakeBatchProcessor.MDC_KEY;
import static com.kmwllc.lucille.core.FakeBatchProcessor.batch;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.MDC;

/**
 * Tests {@link ConcurrentDispatcher} on its own, against a {@link FakeBatchProcessor}, with trivial destination-id and
 * barrier callbacks. These tests pin the dispatcher's scheduling contract — which thread each step runs on, the in-flight
 * window, the same-id and barrier gates, and completion in dispatch order — independently of the Indexer, its batches,
 * and any messenger. How a real Indexer derives destination ids and recognizes a delete-by-query is Lucille logic and is
 * covered by {@link IndexerConcurrencyTest}; here a batch's ids are simply its documents' ids, and a document is a
 * barrier when it carries a marker field.
 *
 * <p> The dispatcher confines its state to the thread that calls it, so every dispatcher call here is made on one
 * dedicated "indexer" thread, while the test thread drives the fake processor (releasing held sends) and asserts. A
 * dispatcher call that may block is submitted as a task whose Future the test inspects to see whether it has returned.
 */
public class ConcurrentDispatcherTest {

  private static final long TIMEOUT_MS = 10000;
  // How long to wait before concluding that a dispatcher call is blocked (it has not returned and no send has started).
  private static final long BLOCKED_SETTLE_MS = 200;
  private static final String RUN_ID = "run1";
  private static final String SEND_THREAD_PREFIX = "Lucille-" + RUN_ID + "-IndexerSend-";
  private static final String INDEXER_THREAD_NAME = "test-indexer";

  // Marks a document as a barrier (the stand-in for a delete-by-query).
  private static final String BARRIER_FIELD = "barrier";
  // Make the id extractor, or the barrier predicate, throw for the document.
  private static final String BAD_ID_FIELD = "badId";
  private static final String BAD_BARRIER_FIELD = "badBarrier";

  private FakeBatchProcessor processor;
  private ConcurrentDispatcher dispatcher;
  private ExecutorService indexerThread;
  private volatile Thread indexerThreadRef;

  @Before
  public void setUp() {
    processor = new FakeBatchProcessor();
    indexerThread = Executors.newSingleThreadExecutor(task -> {
      Thread thread = new Thread(task, INDEXER_THREAD_NAME);
      indexerThreadRef = thread;
      return thread;
    });
  }

  @After
  public void tearDown() throws Exception {
    processor.releaseAll();
    if (dispatcher != null) {
      onIndexerThread(dispatcher::close).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }
    indexerThread.shutdownNow();
    assertTrue(indexerThread.awaitTermination(TIMEOUT_MS, TimeUnit.MILLISECONDS));
  }

  // ---- threading contract ----

  @Test
  public void testSendsRunOnPoolThreadsAndCompletionsOnTheDispatchingThread() throws Exception {
    newDispatcher(2);
    dispatchAndWait(batch("A", "x"));
    dispatchAndWait(batch("B", "y"));
    onIndexerThread(dispatcher::drain).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

    assertEquals(List.of("A", "B"), processor.completed());
    for (String label : List.of("A", "B")) {
      assertTrue("send must run on a pool thread, ran on " + processor.sendThread(label),
          processor.sendThread(label).startsWith(SEND_THREAD_PREFIX));
      assertNotEquals(INDEXER_THREAD_NAME, processor.sendThread(label));
      // The outcome the send returned is the one handed to completion, untouched.
      assertSame(processor.sentOutcome(label), processor.completedOutcome(label));
    }
    assertEquals(Set.of(INDEXER_THREAD_NAME), processor.completionThreads());
    assertFalse("completions must never overlap", processor.completionsOverlapped());
  }

  @Test
  public void testDispatchReturnsWhileTheSendIsStillRunning() throws Exception {
    // The mirror of SynchronousDispatcherTest.testDispatchBlocksUntilTheSendHasCompleted: with room in the window,
    // dispatch hands the send to the pool and returns, leaving the batch in flight and not yet completed.
    newDispatcher(2);
    processor.hold("A");
    dispatchAndWait(batch("A", "x"));
    awaitUntil(() -> processor.hasStartedSend("A"));
    assertFalse("the send must still be running after dispatch returned", processor.hasFinishedSend("A"));
    assertEquals(List.of(), processor.completed());

    processor.release("A");
    onIndexerThread(dispatcher::drain).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    assertEquals(List.of("A"), processor.completed());
  }

  @Test
  public void testMdcIsPropagatedToTheSendThread() throws Exception {
    newDispatcher(1);
    onIndexerThread(() -> {
      MDC.put(MDC_KEY, "value");
      try {
        dispatcher.dispatch(batch("A", "x"));
        dispatcher.drain();
      } finally {
        MDC.remove(MDC_KEY);
      }
    }).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    assertEquals("value", processor.sendMdcValue("A"));
  }

  // ---- window and gates ----

  @Test
  public void testWindowLimitsSendsInFlight() throws Exception {
    newDispatcher(2);
    processor.hold("A", "B");
    dispatchAndWait(batch("A", "x"));
    dispatchAndWait(batch("B", "y"));
    awaitUntil(() -> processor.hasStartedSends("A", "B"));

    // The window is full, so dispatching C blocks without starting its send.
    Future<?> dispatchC = dispatch(batch("C", "z"));
    assertStaysBlocked(dispatchC, "C");
    assertEquals(List.of(), processor.completed());

    // Completing the oldest batch frees a slot: C is sent, and only A was completed to make room.
    processor.release("A");
    dispatchC.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    awaitUntil(() -> processor.hasStartedSend("C"));
    assertEquals(List.of("A"), processor.completed());
    assertEquals(2, processor.peakConcurrentSends());
  }

  @Test
  public void testBatchesWithDisjointIdsRunTogether() throws Exception {
    newDispatcher(3);
    processor.hold("A");
    dispatchAndWait(batch("A", "x"));
    dispatchAndWait(batch("B", "y"));
    awaitUntil(() -> processor.hasStartedSends("A", "B"));
    assertEquals(2, processor.peakConcurrentSends());
  }

  @Test
  public void testBatchSharingAnIdWaitsForThatBatchToComplete() throws Exception {
    newDispatcher(3);
    processor.hold("A", "B");
    dispatchAndWait(batch("A", "x"));
    dispatchAndWait(batch("B", "y"));
    awaitUntil(() -> processor.hasStartedSends("A", "B"));

    // C writes x, which A is still writing: C must wait for A, even though the window has room.
    Future<?> dispatchC = dispatch(batch("C", "x"));
    assertStaysBlocked(dispatchC, "C");

    // Finishing B does not help: it is not the batch C conflicts with, and it is not the oldest, so it is not completed.
    processor.release("B");
    awaitUntil(() -> processor.hasFinishedSend("B"));
    assertStaysBlocked(dispatchC, "C");
    assertEquals(List.of(), processor.completed());

    // Finishing A lets C go. Only A had to be completed; B stays in flight (from the dispatcher's point of view) until
    // something else asks for it.
    processor.release("A");
    dispatchC.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    awaitUntil(() -> processor.hasStartedSend("C"));
    assertEquals(List.of("A"), processor.completed());
  }

  @Test
  public void testBarrierWaitsForEverythingInFlightAndRunsAlone() throws Exception {
    newDispatcher(3);
    processor.hold("A", "B", "D");
    dispatchAndWait(batch("A", "x"));
    dispatchAndWait(batch("B", "y"));
    awaitUntil(() -> processor.hasStartedSends("A", "B"));

    // A barrier shares no id with A or B and the window has room, yet it must wait for both.
    Future<?> dispatchD = dispatch(barrierBatch("D", "z"));
    assertStaysBlocked(dispatchD, "D");
    processor.release("A");
    awaitUntil(() -> processor.hasFinishedSend("A"));
    assertStaysBlocked(dispatchD, "D");

    // With nothing in flight the barrier is sent — and its dispatch still does not return until it has completed, so
    // nothing can be dispatched behind it while it runs.
    processor.release("B");
    awaitUntil(() -> processor.hasStartedSend("D"));
    assertStaysBlocked(dispatchD, "D");
    assertEquals(1, processor.concurrentSends());
    processor.release("D");
    dispatchD.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    assertEquals(List.of("A", "B", "D"), processor.completed());

    // Dispatch resumes normally after the barrier.
    dispatchAndWait(batch("E", "x"));
    onIndexerThread(dispatcher::drain).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    assertEquals(List.of("A", "B", "D", "E"), processor.completed());
  }

  // ---- completion ----

  @Test
  public void testCompletionFollowsDispatchOrderWhenSendsFinishOutOfOrder() throws Exception {
    newDispatcher(3);
    processor.hold("A", "B", "C");
    dispatchAndWait(batch("A", "x"));
    dispatchAndWait(batch("B", "y"));
    dispatchAndWait(batch("C", "z"));
    awaitUntil(() -> processor.hasStartedSends("A", "B", "C"));

    // C and B finish first, but nothing can be completed past A, which is still in flight — and completeFinishedBatches
    // must say so without blocking.
    processor.release("C", "B");
    awaitUntil(() -> processor.hasFinishedSend("C") && processor.hasFinishedSend("B"));
    onIndexerThread(dispatcher::completeFinishedBatches).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    assertEquals(List.of(), processor.completed());

    // Once A finishes, the whole finished prefix completes, in dispatch order.
    processor.release("A");
    awaitUntil(() -> processor.hasFinishedSend("A"));
    onIndexerThread(dispatcher::completeFinishedBatches).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    assertEquals(List.of("A", "B", "C"), processor.completed());
  }

  @Test
  public void testCompleteFinishedBatchesCompletesOnlyTheFinishedPrefix() throws Exception {
    newDispatcher(3);
    processor.hold("B");
    dispatchAndWait(batch("A", "x"));
    dispatchAndWait(batch("B", "y"));
    dispatchAndWait(batch("C", "z"));
    awaitUntil(() -> processor.hasFinishedSend("A") && processor.hasFinishedSend("C"));

    // A is finished and first, so it completes; B is still in flight, so C (finished) must wait behind it.
    onIndexerThread(dispatcher::completeFinishedBatches).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    assertEquals(List.of("A"), processor.completed());

    processor.release("B");
    awaitUntil(() -> processor.hasFinishedSend("B"));
    onIndexerThread(dispatcher::completeFinishedBatches).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    assertEquals(List.of("A", "B", "C"), processor.completed());
  }

  @Test
  public void testDrainWaitsForAndCompletesEverythingInOrder() throws Exception {
    newDispatcher(3);
    processor.hold("A", "B", "C");
    dispatchAndWait(batch("A", "x"));
    dispatchAndWait(batch("B", "y"));
    dispatchAndWait(batch("C", "z"));
    awaitUntil(() -> processor.hasStartedSends("A", "B", "C"));

    Future<?> drain = onIndexerThread(dispatcher::drain);
    assertStaysBlocked(drain, "drain");
    processor.release("C", "B", "A");
    drain.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    assertEquals(List.of("A", "B", "C"), processor.completed());
  }

  @Test
  public void testFailedSendOutcomeReachesCompletionInOrder() throws Exception {
    newDispatcher(2);
    RuntimeException failure = new RuntimeException("send failed");
    processor.failSend("A", failure);
    dispatchAndWait(batch("A", "x"));
    dispatchAndWait(batch("B", "y"));
    onIndexerThread(dispatcher::drain).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

    assertEquals(List.of("A", "B"), processor.completed());
    assertSame(failure, processor.completedOutcome("A").error());
    assertNull(processor.completedOutcome("B").error());
  }

  @Test
  public void testProcessorThatThrowsFromSendStillCompletesTheBatchWithThatError() throws Exception {
    // BatchProcessor.sendWithRetry is specified never to throw. If one does anyway, the pool wraps the throw in an
    // ExecutionException; the dispatcher must still complete the batch, carrying the thrown exception as its error.
    newDispatcher(2);
    processor.throwFromSend("A");
    dispatchAndWait(batch("A", "x"));
    dispatchAndWait(batch("B", "y"));
    onIndexerThread(dispatcher::drain).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

    assertEquals(List.of("A", "B"), processor.completed());
    assertTrue(processor.completedOutcome("A").error() instanceof IllegalStateException);
    assertNull(processor.completedOutcome("B").error());
  }

  // ---- callbacks that throw ----

  @Test
  public void testThrowingIdExtractorFailsTheBatchAfterDrainingItsPredecessors() throws Exception {
    newDispatcher(2);
    processor.hold("A");
    dispatchAndWait(batch("A", "x"));
    awaitUntil(() -> processor.hasStartedSend("A"));

    // The bad batch is failed without being sent, but only after A completes, so completion order is still kept.
    List<Document> bad = batch("Bad", "y");
    bad.get(0).setField(BAD_ID_FIELD, "true");
    Future<?> dispatchBad = dispatch(bad);
    assertStaysBlocked(dispatchBad, "Bad");
    processor.release("A");
    dispatchBad.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

    assertEquals(List.of("A", "Bad"), processor.completed());
    assertFalse("a batch whose ids cannot be extracted must not be sent", processor.hasStartedSend("Bad"));
    assertTrue(processor.completedOutcome("Bad").error() instanceof IllegalArgumentException);

    // The dispatcher is still usable afterwards.
    dispatchAndWait(batch("C", "y"));
    onIndexerThread(dispatcher::drain).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    assertEquals(List.of("A", "Bad", "C"), processor.completed());
  }

  @Test
  public void testThrowingBarrierPredicateFailsTheBatchWithoutSendingIt() throws Exception {
    newDispatcher(2);
    List<Document> bad = batch("Bad", "x");
    bad.get(0).setField(BAD_BARRIER_FIELD, "true");
    dispatchAndWait(bad);

    assertEquals(List.of("Bad"), processor.completed());
    assertFalse(processor.hasStartedSend("Bad"));
    assertTrue(processor.completedOutcome("Bad").error() instanceof IllegalArgumentException);
  }

  // ---- interrupts and shutdown ----

  @Test
  public void testInterruptWhileWaitingIsHeldUntilTheSendFinishesThenRestored() throws Exception {
    newDispatcher(1);
    processor.hold("A");
    dispatchAndWait(batch("A", "x"));
    awaitUntil(() -> processor.hasStartedSend("A"));

    // drain() blocks on A. Interrupting the waiting thread must not abandon the wait: A has already been sent, so its
    // completion is still owed. The flag is restored once the wait ends, so the caller can see it afterwards.
    AtomicBoolean interruptedAfterDrain = new AtomicBoolean();
    Future<?> drain = onIndexerThread(() -> {
      dispatcher.drain();
      interruptedAfterDrain.set(Thread.interrupted()); // read and clear, so the thread is clean for the next task
    });
    assertStaysBlocked(drain, "drain");
    indexerThreadRef.interrupt();
    assertStaysBlocked(drain, "drain");
    assertEquals(List.of(), processor.completed());

    processor.release("A");
    drain.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    assertEquals(List.of("A"), processor.completed());
    assertTrue("the interrupt must be restored after the wait", interruptedAfterDrain.get());
  }

  @Test
  public void testCloseAfterDrainTerminatesThePool() throws Exception {
    newDispatcher(2);
    dispatchAndWait(batch("A", "x"));
    onIndexerThread(dispatcher::drain).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    assertFalse(dispatcher.sendPoolTerminated());
    onIndexerThread(dispatcher::close).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    assertTrue(dispatcher.sendPoolTerminated());
  }

  @Test
  public void testCloseWithSendsInFlightInterruptsThemAndTerminatesThePool() throws Exception {
    // Only an abnormal exit closes the dispatcher with sends in flight; the run is already failing and their results
    // would be discarded, so the sends are interrupted rather than waited for.
    newDispatcher(2);
    processor.hold("A");
    dispatchAndWait(batch("A", "x"));
    awaitUntil(() -> processor.hasStartedSend("A"));

    onIndexerThread(dispatcher::close).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    assertTrue(dispatcher.sendPoolTerminated());
    assertTrue("the held send must have been interrupted", processor.wasInterrupted("A"));
    assertEquals("an interrupted in-flight batch is not completed", List.of(), processor.completed());
  }

  // ---- helpers ----

  private void newDispatcher(int maxConcurrentBatches) {
    dispatcher = new ConcurrentDispatcher(maxConcurrentBatches, RUN_ID, processor,
        ConcurrentDispatcherTest::destinationIds, ConcurrentDispatcherTest::isBarrier);
  }

  /** The destination-id extractor: a batch writes its documents' ids. Throws for a document marked with BAD_ID_FIELD. */
  private static Set<String> destinationIds(List<Document> docs) {
    Set<String> ids = new HashSet<>();
    for (Document doc : docs) {
      if (doc.has(BAD_ID_FIELD)) {
        throw new IllegalArgumentException("cannot extract id of " + doc.getId());
      }
      ids.add(doc.getId());
    }
    return ids;
  }

  /** The barrier predicate: a document marked with BARRIER_FIELD. Throws for one marked with BAD_BARRIER_FIELD. */
  private static boolean isBarrier(Document doc) {
    if (doc.has(BAD_BARRIER_FIELD)) {
      throw new IllegalArgumentException("cannot tell whether " + doc.getId() + " is a barrier");
    }
    return doc.has(BARRIER_FIELD);
  }

  /** A batch whose first document is a barrier. */
  private static List<Document> barrierBatch(String label, String... ids) {
    List<Document> docs = batch(label, ids);
    docs.get(0).setField(BARRIER_FIELD, "true");
    return docs;
  }

  private Future<?> onIndexerThread(Runnable task) {
    return indexerThread.submit(task);
  }

  private Future<?> dispatch(List<Document> docs) {
    return onIndexerThread(() -> dispatcher.dispatch(docs));
  }

  /** Dispatches on the indexer thread and waits for the dispatch call to return (failing if it blocks). */
  private void dispatchAndWait(List<Document> docs) throws Exception {
    dispatch(docs).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  /** Asserts that a dispatcher call submitted to the indexer thread has not returned after a settling period. */
  private static void assertStaysBlocked(Future<?> call, String what) throws InterruptedException {
    Thread.sleep(BLOCKED_SETTLE_MS);
    assertFalse(what + " should still be blocked", call.isDone());
  }

  private static void awaitUntil(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.currentTimeMillis() + TIMEOUT_MS;
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline) {
        fail("timed out waiting for condition");
      }
      Thread.sleep(5);
    }
  }
}
