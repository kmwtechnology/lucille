package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Event.Type;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Test;

/**
 * Tests indexer.maxConcurrentBatches: that batches, though sent concurrently on a pool, are completed in the order they
 * were dispatched, along with the dispatch gates, retries, draining, and error handling.
 *
 * <p> Each document (and so each single-document batch) is identified by its id. A test uses a {@link ControlledIndexer}
 * whose sends block until released, so it can control the order in which sends finish and assert on the resulting start,
 * finish, and completion orders.
 *
 * <p> Adapted from the IndexerConcurrencyTest in kmwtechnology/lucille PR #575.
 */
public class IndexerConcurrencyTest {

  private static final long TIMEOUT_MS = 10000;

  private Thread indexerThread;
  private ControlledIndexer indexer;

  @After
  public void tearDown() throws Exception {
    if (indexer != null) {
      indexer.releaseAllSends();
      indexer.terminate();
    }
    if (indexerThread != null) {
      indexerThread.join(TIMEOUT_MS);
    }
  }

  @Test
  public void testCompletionFollowsDispatchOrder() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    // maxConcurrentBatches = 2, batchSize = 1, so doc1 and doc2 can be in flight at once as separate batches.
    indexer = new ControlledIndexer(config(2), messenger);
    // Hold both sends open so neither can finish until we say so.
    indexer.blockSendsFor("doc1", "doc2");
    // Add doc1 then doc2 to the messenger's in-memory FIFO queue and start the indexer thread; it polls and dispatches
    // them in that order.
    queueDocsAndStartIndexer("doc1", "doc2");
    // Wait until both sends are actually running on the pool, i.e. both batches are in flight.
    awaitUntil(() -> indexer.hasStartedSends("doc1", "doc2"));

    // Let doc2's send finish first, while doc1's is still blocked.
    indexer.releaseSends("doc2");
    // Wait until doc2's send has returned, so the only thing stopping doc2's completion is doc1 being unfinished.
    awaitUntil(() -> indexer.hasFinishedSend("doc2"));
    // Let a full poll cycle pass, giving the indexer the chance to (wrongly) complete doc2 if ordering were not enforced.
    awaitFullPollCycle(messenger);
    // doc2 must not be completed yet: completion is in dispatch order, and doc1 (dispatched first) has not finished.
    assertEquals("doc2 finished first but must not complete ahead of doc1", List.of(), messenger.completedBatches());

    // Now let doc1 finish; doc1 then doc2 should complete, in dispatch order.
    indexer.releaseSends("doc1");
    // Wait for both completions to be recorded.
    awaitUntil(() -> messenger.completedBatches().size() == 2);
    assertEquals(List.of("doc1", "doc2"), messenger.completedBatches());
    // Both sends really did overlap, confirming the test exercised concurrency rather than serial sends.
    assertEquals(2, indexer.peakConcurrentSends());
  }

  @Test
  public void testCapacityLimitsBatchesInFlight() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    // Window of 2: at most two batches may be in flight at once.
    indexer = new ControlledIndexer(config(2), messenger);
    // Hold doc1, doc2, and doc3 open so we control when the window frees up.
    indexer.blockSendsFor("doc1", "doc2", "doc3");
    // Add four docs to the messenger's in-memory FIFO queue; polling them in order, the indexer fills the window with
    // doc1 and doc2, then tries to dispatch doc3.
    queueDocsAndStartIndexer("doc1", "doc2", "doc3", "doc4");
    // Wait until the window is full (doc1 and doc2 both in flight).
    awaitUntil(() -> indexer.hasStartedSends("doc1", "doc2"));

    // Wait until the indexer thread is blocked in dispatch (it has stopped polling): dispatching doc3 must wait for a
    // slot.
    awaitNextBatchBlocked();
    // doc3 must not have started, because the window is full and no in-flight batch has finished.
    assertFalse("doc3 must not start while the window is full", indexer.hasStartedSend("doc3"));
    // The cap held: never more than two sends ran at once.
    assertEquals("never more than maxConcurrentBatches in flight", 2, indexer.peakConcurrentSends());

    // Finish doc1, freeing one slot; doc3 can now be dispatched and start.
    indexer.releaseSends("doc1");
    awaitUntil(() -> indexer.hasStartedSend("doc3"));

    // Finish the rest; all four batches should complete in dispatch order.
    indexer.releaseSends("doc2", "doc3");
    awaitUntil(() -> messenger.completedBatches().size() == 4);
    assertEquals(List.of("doc1", "doc2", "doc3", "doc4"), messenger.completedBatches());
    // The cap still held across the whole run.
    assertEquals(2, indexer.peakConcurrentSends());
  }

  @Test
  public void testOverlappingDestinationIdsCannotBeInFlightTogether() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    // Window of 3, so capacity is not the limiting factor here — only id overlap is.
    indexer = new ControlledIndexer(config(3), messenger);
    // parentA and parentB each write a child document with id "sharedChild", so their destination id sets overlap.
    // disjointDoc shares no id with either.
    indexer.messenger().queueDoc(docWithChild("parentA", "sharedChild"));
    indexer.messenger().queueDoc(Document.create("disjointDoc"));
    indexer.messenger().queueDoc(docWithChild("parentB", "sharedChild"));
    indexer.blockSendsFor("parentA", "disjointDoc", "parentB");
    indexerThread = new Thread(indexer);
    indexerThread.start();

    // parentA and disjointDoc share no destination id, so both go in flight.
    awaitUntil(() -> indexer.hasStartedSends("parentA", "disjointDoc"));
    // The indexer blocks dispatching parentB, since it writes "sharedChild", which parentA (still in flight) also writes.
    awaitNextBatchBlocked();
    assertFalse("parentB must wait: it shares the child id \"sharedChild\" with the in-flight parentA",
        indexer.hasStartedSend("parentB"));

    // Finishing parentA clears the overlap; parentB can now start.
    indexer.releaseSends("parentA");
    awaitUntil(() -> indexer.hasStartedSend("parentB"));

    indexer.releaseSends("disjointDoc", "parentB");
    awaitUntil(() -> messenger.completedBatches().size() == 3);
    assertEquals(List.of("parentA", "disjointDoc", "parentB"), messenger.completedBatches());
  }

  @Test
  public void testGrandchildIdDeterminesOverlap() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    indexer = new ControlledIndexer(config(3), messenger);
    // ancestorA writes a grandchild "sharedGrandchild" (doc -> child -> grandchild); ancestorB writes the same id as
    // its own direct child. Their destination id sets overlap only via the recursively-collected grandchild id.
    indexer.messenger().queueDoc(docWithGrandchild("ancestorA", "childA", "sharedGrandchild"));
    indexer.messenger().queueDoc(docWithChild("ancestorB", "sharedGrandchild"));
    indexer.blockSendsFor("ancestorA", "ancestorB");
    indexerThread = new Thread(indexer);
    indexerThread.start();

    awaitUntil(() -> indexer.hasStartedSend("ancestorA"));
    awaitNextBatchBlocked();
    assertFalse("ancestorB must wait: it writes ancestorA's grandchild id", indexer.hasStartedSend("ancestorB"));

    indexer.releaseSends("ancestorA");
    awaitUntil(() -> indexer.hasStartedSend("ancestorB"));
    indexer.releaseSends("ancestorB");
    awaitUntil(() -> messenger.completedBatches().size() == 2);
    assertEquals(List.of("ancestorA", "ancestorB"), messenger.completedBatches());
  }

  @Test
  public void testIdOverrideFieldDeterminesOverlap() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    // With idOverrideField set, a document's destination id is the value of that field, not its document id.
    indexer = new ControlledIndexer(config(3, Map.of("indexer.idOverrideField", "destId")), messenger);
    // firstDoc and secondDoc have different document ids but the same destination id ("sharedDestId"), via the
    // override field, so they must not overlap.
    Document firstDoc = Document.create("firstDoc");
    firstDoc.setField("destId", "sharedDestId");
    Document secondDoc = Document.create("secondDoc");
    secondDoc.setField("destId", "sharedDestId");
    indexer.messenger().queueDoc(firstDoc);
    indexer.messenger().queueDoc(secondDoc);
    indexer.blockSendsFor("firstDoc", "secondDoc");
    indexerThread = new Thread(indexer);
    indexerThread.start();

    // firstDoc goes in flight; secondDoc must wait, since it writes the same destination id "sharedDestId".
    awaitUntil(() -> indexer.hasStartedSend("firstDoc"));
    awaitNextBatchBlocked();
    assertFalse("secondDoc must wait: it writes the same destination id as firstDoc",
        indexer.hasStartedSend("secondDoc"));

    indexer.releaseSends("firstDoc");
    awaitUntil(() -> indexer.hasStartedSend("secondDoc"));
    indexer.releaseSends("secondDoc");
    awaitUntil(() -> messenger.completedBatches().size() == 2);
    assertEquals(List.of("firstDoc", "secondDoc"), messenger.completedBatches());
  }

  @Test
  public void testDeleteByQueryBatchRunsAlone() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    // Window of 3, so only the delete-by-query barrier — not capacity — can hold batches back.
    indexer = new ControlledIndexer(config(3, deleteByQuerySettings()), messenger);
    // A normal doc, a delete-by-query doc, then another normal doc.
    indexer.messenger().queueDoc(Document.create("normalBefore"));
    indexer.messenger().queueDoc(deleteByQueryDoc("deleteByQuery"));
    indexer.messenger().queueDoc(Document.create("normalAfter"));
    indexer.blockSendsFor("normalBefore", "deleteByQuery", "normalAfter");
    indexerThread = new Thread(indexer);
    indexerThread.start();

    // normalBefore is in flight. The delete-by-query must wait until nothing else is in flight, because it can match
    // documents in any batch.
    awaitUntil(() -> indexer.hasStartedSend("normalBefore"));
    awaitNextBatchBlocked();
    assertFalse("delete-by-query must wait until nothing else is in flight", indexer.hasStartedSend("deleteByQuery"));

    // Once normalBefore completes, the delete-by-query runs — alone.
    indexer.releaseSends("normalBefore");
    awaitUntil(() -> indexer.hasStartedSend("deleteByQuery"));
    awaitNextBatchBlocked();
    assertFalse("nothing may be dispatched behind a delete-by-query until it completes",
        indexer.hasStartedSend("normalAfter"));

    // After the delete-by-query completes, normalAfter can run.
    indexer.releaseSends("deleteByQuery");
    awaitUntil(() -> indexer.hasStartedSend("normalAfter"));

    indexer.releaseSends("normalAfter");
    awaitUntil(() -> messenger.completedBatches().size() == 3);
    assertEquals(List.of("normalBefore", "deleteByQuery", "normalAfter"), messenger.completedBatches());
    // The barrier forced fully serial execution: never more than one send at a time.
    assertEquals(1, indexer.peakConcurrentSends());
  }

  @Test
  public void testDeleteByIdIsNotABarrier() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    indexer = new ControlledIndexer(config(2, deleteByQuerySettings()), messenger);
    // A delete-by-*id* doc (marked for deletion, but without the deleteByField fields) is not a barrier: it targets a
    // single id, so it is gated only by id overlap, like a normal write.
    indexer.messenger().queueDoc(Document.create("normalDoc"));
    indexer.messenger().queueDoc(deleteByIdDoc("deleteById"));
    indexer.blockSendsFor("normalDoc", "deleteById");
    indexerThread = new Thread(indexer);
    indexerThread.start();

    // Both can be in flight at once; the delete-by-id does not force serial execution.
    awaitUntil(() -> indexer.hasStartedSends("normalDoc", "deleteById"));
    assertEquals(2, indexer.peakConcurrentSends());
  }

  @Test
  public void testKeepAliveCalledWhileWaitingForBatch() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    indexer = new ControlledIndexer(config(2), messenger);
    // Hold doc1 so the indexer thread, once the window is full, blocks waiting for it — the window being 2, it blocks
    // trying to dispatch doc3.
    indexer.blockSendsFor("doc1");
    queueDocsAndStartIndexer("doc1", "doc2", "doc3");
    awaitUntil(() -> indexer.hasStartedSends("doc1", "doc2"));
    awaitNextBatchBlocked();

    // While blocked waiting for an in-flight batch, the indexer thread must periodically call messenger.keepAlive(),
    // so a consumer-group member stays in its group during a long wait.
    awaitUntil(() -> messenger.keepAliveCount() >= 3);

    // Releasing doc1 lets everything drain and the keepAlive calls stop.
    indexer.releaseSends("doc1");
    awaitUntil(() -> messenger.completedBatches().size() == 3);
  }

  @Test
  public void testInterruptWhileWaitingStillCompletesInFlightBatches() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    indexer = new ControlledIndexer(config(2), messenger);
    // doc1 and doc2 go in flight; with the window full, the indexer thread blocks dispatching doc3, waiting for doc1.
    indexer.blockSendsFor("doc1");
    queueDocsAndStartIndexer("doc1", "doc2", "doc3", "doc4");
    awaitUntil(() -> indexer.hasStartedSends("doc1", "doc2"));
    awaitNextBatchBlocked();

    // Interrupt the indexer thread while it waits. The wait must absorb the interrupt (not abandon the in-flight batch)
    // and keep waiting; the flag is restored afterwards so the next poll ends the run.
    indexerThread.interrupt();
    // doc3 must not be abandoned: the indexer keeps waiting for doc1 rather than bailing out.
    awaitNextBatchBlocked();
    assertFalse("an interrupt must not abandon an in-flight batch", indexer.hasStartedSend("doc3"));

    // Releasing doc1 lets the held batches complete; the restored interrupt then terminates the indexer after it drains
    // what it is holding.
    indexer.releaseSends("doc1");
    indexerThread.join(TIMEOUT_MS);
    assertFalse(indexerThread.isAlive());
    // doc1, doc2, doc3 were in flight or dispatched and must all have completed, in order.
    assertEquals(List.of("doc1", "doc2", "doc3"), messenger.completedBatches().subList(0, 3));
  }

  @Test
  public void testTerminateDrainsInFlightBatches() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    indexer = new ControlledIndexer(config(3), messenger);
    indexer.blockSendsFor("doc1", "doc2", "doc3");
    queueDocsAndStartIndexer("doc1", "doc2", "doc3");
    awaitUntil(() -> indexer.hasStartedSends("doc1", "doc2", "doc3"));

    // Terminate while all three are in flight. run() must drain them rather than return with batches unaccounted for,
    // so the thread stays alive until we release the sends.
    indexer.terminate();
    awaitNextBatchBlocked();
    assertTrue("run() must not return while batches are still in flight", indexerThread.isAlive());

    indexer.releaseSends("doc1", "doc2", "doc3");
    indexerThread.join(TIMEOUT_MS);
    assertFalse(indexerThread.isAlive());
    assertEquals(List.of("doc1", "doc2", "doc3"), messenger.completedBatches());
    // The send pool is shut down with the indexer.
    assertTrue(indexer.sendPoolTerminated());
  }

  @Test
  public void testRunIterationsDrainsInFlightBatches() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    ControlledIndexer controlled = new ControlledIndexer(config(4), messenger);
    indexer = controlled;
    // Sends are not blocked; run(n) must still drain every dispatched batch before returning.
    for (String docId : List.of("doc1", "doc2", "doc3", "doc4", "doc5")) {
      messenger.queueDoc(Document.create(docId));
    }
    controlled.run(5);
    assertEquals(List.of("doc1", "doc2", "doc3", "doc4", "doc5"), messenger.completedBatches());
    assertTrue(controlled.sendPoolTerminated());
  }

  @Test
  public void testErrorFromSendSurfacesAndStopsPool() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    ControlledIndexer controlled = new ControlledIndexer(config(2), messenger);
    indexer = controlled;
    // The send for doc1 throws an Error (e.g. OutOfMemoryError), which must propagate out of run() on the indexer
    // thread rather than be swallowed.
    controlled.failSend("doc1", new StackOverflowError("boom"));
    messenger.queueDoc(Document.create("doc1"));

    StackOverflowError error = assertThrows(StackOverflowError.class, () -> controlled.run(1));
    assertEquals("boom", error.getMessage());
    // Even on the Error path, the send pool must be shut down rather than left running.
    assertTrue(controlled.sendPoolTerminated());
  }

  @Test
  public void testErrorWithOtherSendsInFlightStopsPoolPromptly() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    ControlledIndexer controlled = new ControlledIndexer(config(2), messenger);
    indexer = controlled;
    // doc1's send throws an Error; doc2's send stays blocked and so is still in flight when the Error propagates out of
    // run(). Closing must not wait out doc2's (never-ending) send — it must interrupt the pool and abandon it.
    controlled.failSend("doc1", new StackOverflowError("boom"));
    controlled.blockSendsFor("doc2");
    messenger.queueDoc(Document.create("doc1"));
    messenger.queueDoc(Document.create("doc2"));

    long start = System.currentTimeMillis();
    assertThrows(StackOverflowError.class, () -> controlled.run(2));
    long elapsedMs = System.currentTimeMillis() - start;

    // The pool is interrupted and stops promptly, not after the full shutdown timeout. (run() returns only once close()
    // has shut the pool down, so by here it is terminated.)
    assertTrue(controlled.sendPoolTerminated());
    assertTrue("close() must not wait out an in-flight send on the Error path (took " + elapsedMs + " ms)",
        elapsedMs < 5000);
  }

  @Test
  public void testRetryRunsOnPoolWithoutReorderingCompletion() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    // Retries enabled with a tiny wait; K=2 so both batches can be in flight while doc1 retries on the pool.
    ControlledIndexer controlled = new ControlledIndexer(
        config(2, Map.of("indexer.maxRetries", 3, "indexer.retryWaitDurationMs", 1)), messenger);
    indexer = controlled;
    // doc1's send fails retryably twice (then succeeds); doc2 is held so it is still in flight while doc1 retries.
    controlled.failSendRetryablyThenSucceed("doc1", 2);
    controlled.blockSendsFor("doc2");
    queueDocsAndStartIndexer("doc1", "doc2");

    awaitUntil(() -> indexer.hasStartedSends("doc1", "doc2"));
    // doc1 finished (after its retries), but must not complete ahead of... it is first, so release doc2 and both
    // complete in dispatch order.
    indexer.releaseSends("doc2");
    awaitUntil(() -> messenger.completedBatches().size() == 2);

    assertEquals(List.of("doc1", "doc2"), messenger.completedBatches());
    assertEquals(List.of("FINISH:doc1", "FINISH:doc2"), messenger.sentEvents());
    // doc1 was attempted three times: two retryable failures plus the successful attempt.
    assertEquals(3, controlled.sendAttempts("doc1"));
  }

  @Test
  public void testPerDocumentFailureInConcurrentBatch() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    // batchSize 2, so okDoc and failedDoc go in the same batch; K=2 sends it on the pool.
    ControlledIndexer controlled = new ControlledIndexer(config(2, Map.of("indexer.batchSize", 2)), messenger);
    indexer = controlled;
    // The destination rejects failedDoc individually; okDoc succeeds in the same batch.
    controlled.failDocument("failedDoc");
    messenger.queueDoc(Document.create("okDoc"));
    messenger.queueDoc(Document.create("failedDoc"));

    controlled.run(2);
    // The batch completes, and events reflect the per-document split: FAIL for the rejected doc, FINISH for the other.
    assertEquals(List.of("okDoc,failedDoc"), messenger.completedBatches());
    assertEquals(Set.of("FAIL:failedDoc", "FINISH:okDoc"), Set.copyOf(messenger.sentEvents()));
  }

  @Test
  public void testSlowSynchronousSendDoesNotSplitBatches() throws Exception {
    assertFullBatchesDespiteSlowSends(1);
  }

  @Test
  public void testWaitForFreeSlotDoesNotSplitBatches() throws Exception {
    assertFullBatchesDespiteSlowSends(2);
  }

  // A send slower than batchTimeout must not make the next add flush the leftover document as a batch of its own: time
  // the indexer thread spends blocked sending (K=1) or waiting for a free slot (K>1) is excluded from the batch timer.
  private void assertFullBatchesDespiteSlowSends(int maxConcurrentBatches) throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    ControlledIndexer controlled = new ControlledIndexer(
        config(maxConcurrentBatches, Map.of("indexer.batchSize", 2, "indexer.batchTimeout", 50)), messenger);
    indexer = controlled;
    // Each send takes far longer than the 50 ms batchTimeout.
    controlled.setSendDelayMs(200);
    for (String docId : List.of("doc1", "doc2", "doc3", "doc4", "doc5", "doc6")) {
      messenger.queueDoc(Document.create(docId));
    }

    controlled.run(6);
    // Batches must stay full (two docs each), not fragment into singles because a slow send let the timer expire.
    assertEquals(List.of("doc1,doc2", "doc3,doc4", "doc5,doc6"), messenger.completedBatches());
  }

  // --- helpers ---

  private static Config config(int maxConcurrentBatches) {
    return config(maxConcurrentBatches, Map.of());
  }

  /** Base config (batchSize 1, short batchTimeout) with the given maxConcurrentBatches, plus any extra settings. */
  private static Config config(int maxConcurrentBatches, Map<String, Object> extraSettings) {
    Map<String, Object> settings = new HashMap<>(Map.of(
        "indexer.batchSize", 1, "indexer.batchTimeout", 20, "indexer.maxConcurrentBatches", maxConcurrentBatches));
    settings.putAll(extraSettings);
    return ConfigFactory.parseMap(settings);
  }

  /** A document with the given id that carries one child document with the given child id. */
  private static Document docWithChild(String docId, String childId) {
    Document doc = Document.create(docId);
    doc.addChild(Document.create(childId));
    return doc;
  }

  /** A document with the given id carrying a child, which in turn carries a grandchild with the given id. */
  private static Document docWithGrandchild(String docId, String childId, String grandchildId) {
    Document doc = Document.create(docId);
    Document child = Document.create(childId);
    child.addChild(Document.create(grandchildId));
    doc.addChild(child);
    return doc;
  }

  /** Indexer settings that enable deletion handling, so a document can be a delete-by-id or delete-by-query request. */
  private static Map<String, Object> deleteByQuerySettings() {
    return Map.of(
        "indexer.deletionMarkerField", "isDeleted",
        "indexer.deletionMarkerFieldValue", "true",
        "indexer.deleteByFieldField", "deleteField",
        "indexer.deleteByFieldValue", "deleteValue");
  }

  /** A document marked for deletion and carrying the deleteByField fields, making it a delete-by-query request. */
  private static Document deleteByQueryDoc(String docId) {
    Document doc = deleteByIdDoc(docId);
    doc.setField("deleteField", "someField");
    doc.setField("deleteValue", "someValue");
    return doc;
  }

  /** A document marked for deletion but without the deleteByField fields, making it a delete-by-id request. */
  private static Document deleteByIdDoc(String docId) {
    Document doc = Document.create(docId);
    doc.setField("isDeleted", "true");
    return doc;
  }

  /** Queues a document for each of the given ids on the messenger, then starts the indexer thread. */
  private void queueDocsAndStartIndexer(String... docIds) {
    for (String docId : docIds) {
      indexer.messenger().queueDoc(Document.create(docId));
    }
    indexerThread = new Thread(indexer);
    indexerThread.start();
  }

  /** Blocks until the condition holds, failing if it does not within {@link #TIMEOUT_MS}. */
  private static void awaitUntil(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.currentTimeMillis() + TIMEOUT_MS;
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("Condition not met within " + TIMEOUT_MS + " ms");
      }
      Thread.sleep(5);
    }
  }

  // Waits until the indexer has started two more polls, so a full poll cycle (which, at the start of each poll,
  // completes any finished batches) has elapsed since this call.
  private static void awaitFullPollCycle(RecordingMessenger messenger) throws InterruptedException {
    int start = messenger.pollCount();
    awaitUntil(() -> messenger.pollCount() >= start + 2);
  }

  // Waits until the next batch cannot start: the indexer thread is blocked inside dispatch(), held by the window being
  // full, an id overlap, or a pending delete-by-query barrier. This makes a following assertion that some batch has
  // not started reflect the gate holding it back, rather than mere timing.
  //
  // The blocked state is detected indirectly: a thread blocked in dispatch() is not polling, so the poll count holds
  // steady. We wait until it has not advanced across a short window. (Safe here because a test always has documents
  // queued, so the only reason polling stops is the dispatch() block.)
  private void awaitNextBatchBlocked() throws InterruptedException {
    RecordingMessenger messenger = indexer.messenger();
    long deadline = System.currentTimeMillis() + TIMEOUT_MS;
    while (System.currentTimeMillis() < deadline) {
      int before = messenger.pollCount();
      Thread.sleep(100);
      if (messenger.pollCount() == before) {
        return;
      }
    }
    throw new AssertionError("next batch was not blocked within " + TIMEOUT_MS + " ms");
  }

  /**
   * An Indexer whose every send blocks until the test releases it, letting a test drive the order in which concurrent
   * sends finish.
   *
   * <p> {@link #blockSendsFor(String...)} installs a latch per document id; {@link #sendToIndex(List)} waits on the
   * latch until {@link #releaseSends(String...)} opens it. {@link #sendsStarted} and {@link #sendsFinished} record, in
   * order, the document ids whose sends have begun and finished, and {@link #peakConcurrentSends} records the
   * high-water mark of simultaneous sends.
   *
   * <p> A batch is identified and gated by the id of its <b>first</b> document. With batchSize 1 (as most of these
   * tests use) that is simply the batch's one document. A test that uses multi-document batches should therefore gate
   * on, and track by, the first document's id — the ids of later documents in the same batch are not used for blocking
   * or for the started/finished records (though {@link RecordingMessenger#batchComplete} still records the full batch).
   */
  static class ControlledIndexer extends Indexer {

    public static final Spec SPEC = SpecBuilder.indexer().build();

    private final IndexerMessenger messenger;
    // Document ids whose sendToIndex has begun / finished, in order.
    private final List<String> sendsStarted = Collections.synchronizedList(new ArrayList<>());
    private final List<String> sendsFinished = Collections.synchronizedList(new ArrayList<>());
    // High-water mark of sends running at the same time.
    private final AtomicInteger peakConcurrentSends = new AtomicInteger();
    private final AtomicInteger concurrentSends = new AtomicInteger();
    // Per document id, a latch its send waits on; absent means the send does not block.
    private final Map<String, CountDownLatch> sendLatches = new ConcurrentHashMap<>();
    // When positive, every send sleeps this many milliseconds (simulating a slow destination).
    private volatile long sendDelayMs = 0;
    // Per document id, a Throwable its send should throw (after any latch is released) instead of succeeding.
    private final Map<String, Throwable> sendFailures = new ConcurrentHashMap<>();
    // Document ids that sendToIndex reports as per-document failures (returned in the failed set, not thrown).
    private final Set<String> failedDocIds = ConcurrentHashMap.newKeySet();
    // Per document id, how many more send attempts should throw a retryable exception before succeeding.
    private final Map<String, AtomicInteger> retryableFailures = new ConcurrentHashMap<>();
    // Per document id, the total number of send attempts made (to assert that retries actually occurred).
    private final Map<String, AtomicInteger> sendAttempts = new ConcurrentHashMap<>();

    ControlledIndexer(Config config, RecordingMessenger messenger) {
      super(config, messenger, false, "IndexerConcurrencyTest", "IndexerConcurrencyTest");
      this.messenger = messenger;
    }

    /** The messenger this indexer was built with, for the test to queue documents and read recorded output. */
    RecordingMessenger messenger() {
      return (RecordingMessenger) messenger;
    }

    /** Makes the send of each given document id block until {@link #releaseSends(String...)} is called for it. */
    void blockSendsFor(String... docIds) {
      for (String docId : docIds) {
        sendLatches.put(docId, new CountDownLatch(1));
      }
    }

    /** Unblocks the sends of the given document ids. */
    void releaseSends(String... docIds) {
      for (String docId : docIds) {
        sendLatches.get(docId).countDown();
      }
    }

    /** Unblocks every send, used in teardown so a test never leaves the indexer thread blocked. */
    void releaseAllSends() {
      sendLatches.values().forEach(CountDownLatch::countDown);
    }

    /** Makes the send of the given document id throw the given Throwable (after any latch is released). */
    void failSend(String docId, Throwable failure) {
      sendFailures.put(docId, failure);
    }

    /** Makes every send sleep this many milliseconds, simulating a slow destination. */
    void setSendDelayMs(long sendDelayMs) {
      this.sendDelayMs = sendDelayMs;
    }

    /** Makes sendToIndex report the given document id as a per-document failure (returned, not thrown). */
    void failDocument(String docId) {
      failedDocIds.add(docId);
    }

    /** Makes the next {@code count} send attempts for the given document id throw a retryable exception, then succeed. */
    void failSendRetryablyThenSucceed(String docId, int count) {
      retryableFailures.put(docId, new AtomicInteger(count));
    }

    /** The number of send attempts made for the given document id (initial attempt plus retries). */
    int sendAttempts(String docId) {
      AtomicInteger attempts = sendAttempts.get(docId);
      return attempts == null ? 0 : attempts.get();
    }

    /** Whether the sends of all the given document ids have begun. */
    boolean hasStartedSends(String... docIds) {
      return sendsStarted.containsAll(List.of(docIds));
    }

    /** Whether the send of the given document id has begun. */
    boolean hasStartedSend(String docId) {
      return sendsStarted.contains(docId);
    }

    /** Whether the send of the given document id has finished. */
    boolean hasFinishedSend(String docId) {
      return sendsFinished.contains(docId);
    }

    /** The high-water mark of sends that ran at the same time. */
    int peakConcurrentSends() {
      return peakConcurrentSends.get();
    }

    @Override
    protected boolean supportsConcurrentSends() {
      return true;
    }

    @Override
    protected String getIndexerConfigKey() {
      return null;
    }

    @Override
    public boolean validateConnection() {
      return true;
    }

    @Override
    public void closeConnection() {
    }

    @Override
    protected Set<Pair<Document, Exception>> sendToIndex(List<Document> documents) throws Exception {
      String docId = documents.get(0).getId();
      sendAttempts.computeIfAbsent(docId, k -> new AtomicInteger()).incrementAndGet();
      peakConcurrentSends.accumulateAndGet(concurrentSends.incrementAndGet(), Math::max);
      if (!sendsStarted.contains(docId)) {
        sendsStarted.add(docId);
      }
      try {
        CountDownLatch latch = sendLatches.get(docId);
        if (latch != null && !latch.await(TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
          throw new IllegalStateException("send latch for " + docId + " was never released");
        }
        if (sendDelayMs > 0) {
          Thread.sleep(sendDelayMs);
        }
        // Throw a retryable exception for the first N attempts of this document, so the base-class retry policy re-runs
        // the send; the attempt counter proves the retries happened on the send pool.
        AtomicInteger remainingRetryableFailures = retryableFailures.get(docId);
        if (remainingRetryableFailures != null && remainingRetryableFailures.getAndDecrement() > 0) {
          throw new IndexerRetryableException(503, "retryable failure for " + docId, null);
        }
        // A configured failure is held as a Throwable, which cannot be rethrown directly (sendToIndex declares only
        // throws Exception, not Throwable). Split by type so each is thrown as itself: Error unchecked, Exception as the
        // declared checked type. This lets a test simulate either an unchecked Error (e.g. OutOfMemoryError) or a normal
        // send Exception.
        Throwable failure = sendFailures.get(docId);
        if (failure instanceof Error error) {
          throw error;
        } else if (failure instanceof Exception exception) {
          throw exception;
        }
        // Report any configured per-document failures as returned pairs (not thrown), as a real sendToIndex does for
        // documents the destination rejected individually.
        return documents.stream()
            .filter(d -> failedDocIds.contains(d.getId()))
            .map(d -> Pair.<Document, Exception>of(d, new IndexerException("rejected " + d.getId())))
            .collect(Collectors.toSet());
      } finally {
        concurrentSends.decrementAndGet();
        if (!sendsFinished.contains(docId)) {
          sendsFinished.add(docId);
        }
      }
    }
  }

  /**
   * An IndexerMessenger backed by an in-memory queue that records sent events and batch completions in order.
   *
   * <p> The shared {@link com.kmwllc.lucille.message.TestMessenger} is not used here because it records neither the
   * order in which {@link #batchComplete(List)} is called — which is exactly what these tests assert — nor the number
   * of polls, which {@link #awaitFullPollCycle} and {@link #awaitNextBatchBlocked} need. Those observations are
   * specific to concurrency testing, so they live in this local double rather than being added to the widely used
   * TestMessenger.
   */
  static class RecordingMessenger implements IndexerMessenger {

    // In-memory FIFO queue of documents waiting to be polled for indexing, in the role a Kafka topic or LocalMessenger
    // queue plays in production.
    private final LinkedBlockingQueue<Document> docsToIndex = new LinkedBlockingQueue<>();
    // Number of times pollDocToIndex has been called.
    private final AtomicInteger pollCount = new AtomicInteger();
    // Events sent, as "TYPE:docId", in order.
    private final List<String> sentEvents = Collections.synchronizedList(new ArrayList<>());
    // Completed batches, each rendered as the comma-joined ids of its documents, in completion order.
    private final List<String> completedBatches = Collections.synchronizedList(new ArrayList<>());
    // Number of times keepAlive has been called.
    private final AtomicInteger keepAliveCount = new AtomicInteger();

    /** Adds a document to the in-memory FIFO queue, to be returned (in insertion order) by a later {@link #pollDocToIndex()}. */
    void queueDoc(Document document) {
      docsToIndex.add(document);
    }

    /** The number of times {@link #pollDocToIndex()} has been called. */
    int pollCount() {
      return pollCount.get();
    }

    /** The number of times {@link #keepAlive()} has been called. */
    int keepAliveCount() {
      return keepAliveCount.get();
    }

    List<String> sentEvents() {
      return new ArrayList<>(sentEvents);
    }

    List<String> completedBatches() {
      return new ArrayList<>(completedBatches);
    }

    @Override
    public Document pollDocToIndex() throws Exception {
      pollCount.incrementAndGet();
      return docsToIndex.poll(10, TimeUnit.MILLISECONDS);
    }

    @Override
    public void sendEvent(Event event) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void sendEvent(Document document, String message, Type type) {
      sentEvents.add(type + ":" + document.getId());
    }

    @Override
    public void sendEvents(List<Document> documents, String message, Type type) {
      for (Document document : documents) {
        sentEvents.add(type + ":" + document.getId());
      }
    }

    @Override
    public void close() {
    }

    @Override
    public void batchComplete(List<Document> batch) throws InterruptedException {
      // Mirror a real messenger (e.g. HybridIndexerMessenger) whose batchComplete makes an interruptible blocking call:
      // this throws InterruptedException if the thread's interrupt flag is set, so a test can prove completion runs with
      // the flag clear and is not lost when an interrupt arrived during the preceding wait.
      new CountDownLatch(1).await(1, TimeUnit.MILLISECONDS);
      completedBatches.add(batch.stream().map(Document::getId).collect(Collectors.joining(",")));
    }

    @Override
    public void keepAlive() {
      keepAliveCount.incrementAndGet();
    }

    // Records batch completions in order and commits nothing at poll, so it is safe for concurrent batches.
    @Override
    public boolean supportsConcurrentBatches() {
      return true;
    }
  }
}
