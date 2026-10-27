package com.kmwllc.lucille.core;

import static com.kmwllc.lucille.core.FakeBatchProcessor.batch;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests {@link SynchronousDispatcher} on its own, against a {@link FakeBatchProcessor}. The synchronous dispatcher is
 * used when indexer.maxConcurrentBatches is 1: each batch is sent and completed on the calling thread, one after the
 * other, so nothing is ever in flight once {@link SynchronousDispatcher#dispatch} returns. These tests pin that, and
 * that its other {@link BatchDispatcher} methods are harmless no-ops.
 */
public class SynchronousDispatcherTest {

  private static final long TIMEOUT_MS = 10000;
  // How long to wait before concluding that a dispatch call is blocked.
  private static final long BLOCKED_SETTLE_MS = 200;

  private FakeBatchProcessor processor;
  private SynchronousDispatcher dispatcher;

  @Before
  public void setUp() {
    processor = new FakeBatchProcessor();
    dispatcher = new SynchronousDispatcher(processor);
  }

  @Test
  public void testDispatchSendsThenCompletesOnTheCallingThread() {
    String callingThread = Thread.currentThread().getName();
    dispatcher.dispatch(batch("A", "x"));
    dispatcher.dispatch(batch("B", "y"));

    // Each batch was sent and completed, in order, on this thread, before dispatch returned.
    assertEquals(List.of("A", "B"), processor.completed());
    for (String label : List.of("A", "B")) {
      assertEquals(callingThread, processor.sendThread(label));
      assertEquals(callingThread, processor.completionThread(label));
    }
    assertEquals("sends never overlap", 1, processor.peakConcurrentSends());
  }

  @Test
  public void testDispatchBlocksUntilTheSendHasCompleted() throws Exception {
    // The Indexer relies on this at maxConcurrentBatches 1: once dispatch returns, nothing is in flight, so drain() has
    // nothing to do and a partition revocation or shutdown never has to wait on a send.
    processor.hold("A");
    ExecutorService caller = Executors.newSingleThreadExecutor();
    try {
      Future<?> dispatchA = caller.submit(() -> dispatcher.dispatch(batch("A", "x")));
      awaitUntil(() -> processor.hasStartedSend("A"));
      Thread.sleep(BLOCKED_SETTLE_MS);
      assertFalse("dispatch must not return while the send is still running", dispatchA.isDone());
      assertEquals(List.of(), processor.completed());

      processor.release("A");
      dispatchA.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertEquals(List.of("A"), processor.completed());
    } finally {
      processor.releaseAll();
      caller.shutdownNow();
    }
  }

  @Test
  public void testOutcomeIsPassedToCompletionUnchanged() {
    RuntimeException failure = new RuntimeException("send failed");
    processor.failSend("A", failure);
    dispatcher.dispatch(batch("A", "x"));
    dispatcher.dispatch(batch("B", "y"));

    assertSame(processor.sentOutcome("A"), processor.completedOutcome("A"));
    assertSame(failure, processor.completedOutcome("A").error());
    assertSame(processor.sentOutcome("B"), processor.completedOutcome("B"));
    assertNull(processor.completedOutcome("B").error());
  }

  @Test
  public void testOtherMethodsAreNoOpsAtAnyTime() {
    // Nothing is ever held in flight, so there is nothing for these to do, before or after any dispatch, and they may
    // be called repeatedly.
    dispatcher.completeFinishedBatches();
    dispatcher.drain();
    dispatcher.close();
    dispatcher.dispatch(batch("A", "x"));
    dispatcher.completeFinishedBatches();
    dispatcher.drain();
    dispatcher.close();
    dispatcher.close();

    assertEquals(List.of("A"), processor.completed());
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
