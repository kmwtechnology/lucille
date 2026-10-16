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
import java.util.Random;
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
  public void testAlternatingSameIdOperationsStaySerializedInOrder() throws Exception {
    // An alternating add/delete/add/delete sequence for the SAME destination id must never overlap and must complete in
    // submission order, even at K > 1 — otherwise a delete could race the add before or after it and leave the wrong
    // final state. The four operations are separate single-document batches with distinct document ids (so the harness
    // can block and track each one) but the same destination id via idOverrideField, so the dispatcher's id-overlap
    // gate is what serializes them. (The ControlledIndexer send is a stub; what is under test is the dispatcher
    // ordering guarantee that makes a real add/delete sequence safe, not the backend's add-vs-delete semantics.)
    RecordingMessenger messenger = new RecordingMessenger();
    // Window of 4, so capacity never limits concurrency here — only the shared destination id does.
    indexer = new ControlledIndexer(config(4, Map.of("indexer.idOverrideField", "destId")), messenger);

    List<String> ops = List.of("add1", "delete1", "add2", "delete2");
    for (String op : ops) {
      indexer.messenger().queueDoc(opDoc(op, "sharedId", op.startsWith("add") ? "add" : "delete", op.substring(op.length() - 1)));
    }
    indexer.blockSendsFor(ops.toArray(new String[0]));
    indexerThread = new Thread(indexer);
    indexerThread.start();

    // Each operation becomes runnable only once the previous one is released and completes: at every step exactly one
    // is in flight, the next is blocked by the shared-id overlap gate, and completion happens in submission order.
    for (int i = 0; i < ops.size(); i++) {
      String current = ops.get(i);
      awaitUntil(() -> indexer.hasStartedSend(current));
      if (i + 1 < ops.size()) {
        String next = ops.get(i + 1);
        awaitNextBatchBlocked();
        assertFalse(next + " must wait: it writes the same destination id as the in-flight " + current,
            indexer.hasStartedSend(next));
      }
      indexer.releaseSends(current);
      int completedSoFar = i + 1;
      awaitUntil(() -> messenger.completedBatches().size() == completedSoFar);
    }

    // Never more than one same-id operation in flight at a time, and all completed in submission order.
    assertEquals(1, indexer.peakConcurrentSends());
    assertEquals(ops, messenger.completedBatches());
    // The operations were also applied to the destination in submission order, and since the last one was a delete, the
    // id ends up absent rather than holding either add's content.
    assertEquals(ops, indexer.appliedOps("sharedId"));
    assertEquals(Map.of(), indexer.destinationContent());
  }

  @Test
  public void testSameIdEndingInAddLeavesTheLatestAddsContent() throws Exception {
    // add(v1), delete, add(v2) for one destination id: the destination must end up holding the LAST add's content (v2),
    // not the first add's (v1) and not nothing. Sends are given delays that would make the earlier operations finish
    // last if they were allowed to run together, so only the id-overlap gate can produce the right final state.
    RecordingMessenger messenger = new RecordingMessenger();
    indexer = new ControlledIndexer(config(3, Map.of("indexer.idOverrideField", "destId")), messenger);
    indexer.messenger().queueDoc(opDoc("add1", "sharedId", "add", "1"));
    indexer.messenger().queueDoc(opDoc("delete1", "sharedId", "delete", null));
    indexer.messenger().queueDoc(opDoc("add2", "sharedId", "add", "2"));
    indexer.setSendDelayMs("add1", 150);
    indexer.setSendDelayMs("delete1", 75);
    indexerThread = new Thread(indexer);
    indexerThread.start();

    awaitUntil(() -> messenger.completedBatches().size() == 3);
    assertEquals(List.of("add1", "delete1", "add2"), indexer.appliedOps("sharedId"));
    assertEquals(Map.of("sharedId", "2"), indexer.destinationContent());
    assertEquals(1, indexer.peakConcurrentSends());
  }

  @Test
  public void testShortSameIdSequencesIncludingDeleteFirstApplyInOrder() throws Exception {
    // Short same-id sequences across batches, including ones that begin with a delete of an id that was never added (a
    // delete can arrive before the document it targets). "aN" is an add carrying version N, "d" is a delete. Each
    // sequence runs on a fresh indexer, with earlier operations given longer sends so that only the id-overlap gate can
    // keep them in order. Each id must end in the state of its LAST operation: a delete leaves it absent, an add leaves
    // that add's version (never an earlier add's).
    List<List<String>> sequences = List.of(
        List.of("d", "a1", "d"),
        List.of("d", "a1"),
        List.of("a1", "d"),
        List.of("d", "d"),
        List.of("a1", "a2"),
        // Runs of repeated adds and deletes, not just strict alternation.
        List.of("d", "a1", "a2", "a3", "d", "d", "a4", "d", "d", "d", "a6", "d", "a7"));
    for (List<String> sequence : sequences) {
      RecordingMessenger messenger = new RecordingMessenger();
      ControlledIndexer sequenceIndexer =
          new ControlledIndexer(config(3, Map.of("indexer.idOverrideField", "destId")), messenger);
      indexer = sequenceIndexer;  // so tearDown releases and terminates it if an assertion fails
      List<String> docIds = new ArrayList<>();
      String expectedVersion = null;
      for (int i = 0; i < sequence.size(); i++) {
        String op = sequence.get(i);
        String docId = "op" + i;
        docIds.add(docId);
        if (op.equals("d")) {
          sequenceIndexer.messenger().queueDoc(opDoc(docId, "sharedId", "delete", null));
          expectedVersion = null;
        } else {
          sequenceIndexer.messenger().queueDoc(opDoc(docId, "sharedId", "add", op.substring(1)));
          expectedVersion = op.substring(1);
        }
        sequenceIndexer.setSendDelayMs(docId, (sequence.size() - i) * 8L);
      }
      indexerThread = new Thread(sequenceIndexer);
      indexerThread.start();

      awaitUntil(() -> messenger.completedBatches().size() == sequence.size());
      assertEquals("applied order for " + sequence, docIds, sequenceIndexer.appliedOps("sharedId"));
      Map<String, String> expectedContent = expectedVersion == null ? Map.of() : Map.of("sharedId", expectedVersion);
      assertEquals("final content for " + sequence, expectedContent, sequenceIndexer.destinationContent());
      assertEquals("same-id ops must never overlap for " + sequence, 1, sequenceIndexer.peakConcurrentSends());

      sequenceIndexer.terminate();
      indexerThread.join(TIMEOUT_MS);
    }
  }

  @Test
  public void testLongMixedSequencesAcrossIdsApplyInOrderPerId() throws Exception {
    // A long, seeded-random run of adds and deletes over several destination ids, with a window (3) smaller than the
    // number of operations and than the number of ids' combined backlog, so unrelated ids overlap freely while each
    // id's own operations must stay serialized. Earlier operations are given longer sends, so any reordering of
    // same-id operations would show up in the applied order and the final content.
    RecordingMessenger messenger = new RecordingMessenger();
    indexer = new ControlledIndexer(config(3, Map.of("indexer.idOverrideField", "destId")), messenger);
    String[] destIds = {"x", "y", "z", "w"};
    int total = 24;
    Random random = new Random(7);
    Map<String, List<String>> expectedOrder = new HashMap<>();
    Map<String, String> expectedContent = new HashMap<>();
    for (int i = 0; i < total; i++) {
      String destId = destIds[random.nextInt(destIds.length)];
      boolean isAdd = random.nextInt(3) != 0;
      String docId = "op" + i;
      indexer.messenger().queueDoc(opDoc(docId, destId, isAdd ? "add" : "delete", isAdd ? String.valueOf(i) : null));
      indexer.setSendDelayMs(docId, (total - i) * 2L);
      expectedOrder.computeIfAbsent(destId, k -> new ArrayList<>()).add(docId);
      if (isAdd) {
        expectedContent.put(destId, String.valueOf(i));
      } else {
        expectedContent.remove(destId);
      }
    }
    indexerThread = new Thread(indexer);
    indexerThread.start();

    awaitUntil(() -> messenger.completedBatches().size() == total);
    for (Map.Entry<String, List<String>> entry : expectedOrder.entrySet()) {
      assertEquals("operations on " + entry.getKey() + " must be applied in submission order",
          entry.getValue(), indexer.appliedOps(entry.getKey()));
    }
    assertEquals(expectedContent, indexer.destinationContent());
    // Unrelated ids really did overlap (the test is not trivially serial), yet completion stayed in dispatch order.
    assertTrue("expected some concurrency across ids", indexer.peakConcurrentSends() > 1);
    List<String> submission = new ArrayList<>();
    for (int i = 0; i < total; i++) {
      submission.add("op" + i);
    }
    assertEquals(submission, messenger.completedBatches());
  }

  @Test
  public void testDeleteByQueryBarrierSeparatesEarlierAndLaterSameIdWrites() throws Exception {
    // add(x), add(y), deleteByQuery, add(x again): the delete-by-query is a barrier, so it applies only after the earlier
    // adds have been applied (clearing them) and before the later add (which therefore survives). The first add is the
    // slowest, so if the barrier did not wait for it, the add would land after the delete-by-query and wrongly survive.
    RecordingMessenger messenger = new RecordingMessenger();
    Map<String, Object> settings = new HashMap<>(deleteByQuerySettings());
    settings.put("indexer.idOverrideField", "destId");
    indexer = new ControlledIndexer(config(3, settings), messenger);
    indexer.messenger().queueDoc(opDoc("addX1", "x", "add", "1"));
    indexer.messenger().queueDoc(opDoc("addY1", "y", "add", "1"));
    Document deleteByQuery = deleteByQueryDoc("dbq");
    deleteByQuery.setField("destId", "q");
    deleteByQuery.setField("op", "deleteByQuery");
    indexer.messenger().queueDoc(deleteByQuery);
    indexer.messenger().queueDoc(opDoc("addX2", "x", "add", "2"));
    indexer.setSendDelayMs("addX1", 200);
    indexer.setSendDelayMs("addY1", 100);
    indexerThread = new Thread(indexer);
    indexerThread.start();

    awaitUntil(() -> messenger.completedBatches().size() == 4);
    assertEquals(List.of("addX1", "addY1", "dbq", "addX2"), messenger.completedBatches());
    assertEquals(List.of("addX1", "addX2"), indexer.appliedOps("x"));
    assertEquals(Map.of("x", "2"), indexer.destinationContent());
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
  public void testErrorFromSendDoesNotCompleteTheBatch() throws Exception {
    // An Error means the batch's documents were never indexed. The batch must NOT be marked complete: with
    // commit-on-completion, completing it would commit the offsets of unindexed documents and silently lose them.
    // But a FAIL event must still be sent for every document, so the run's publisher stops tracking them and does not
    // hang forever (which would otherwise happen in every deployment mode, including local where batchComplete is a
    // no-op and there is no offset replay to recover the batch).
    for (int maxConcurrentBatches : new int[] {1, 2}) {
      RecordingMessenger messenger = new RecordingMessenger();
      ControlledIndexer controlled = new ControlledIndexer(config(maxConcurrentBatches), messenger);
      indexer = controlled;
      controlled.failSend("doc1", new OutOfMemoryError("boom"));
      messenger.queueDoc(Document.create("doc1"));

      assertThrows(OutOfMemoryError.class, () -> controlled.run(1));
      assertTrue("an Error must not mark the batch complete (K=" + maxConcurrentBatches + ")",
          messenger.completedBatches().isEmpty());
      assertEquals("an Error must still FAIL its documents so the run can terminate (K=" + maxConcurrentBatches + ")",
          List.of("FAIL:doc1"), messenger.sentEvents());
      indexer = null;
    }
  }

  @Test
  public void testThrowingIdExtractorFailsBatchWithoutStalling() throws Exception {
    // The concurrent dispatcher calls the id extractor (Indexer::destinationIds) before a batch is sent. If it throws —
    // e.g. a malformed idOverrideField value — the throw must not escape and kill the indexer thread with the batch's
    // documents never completed (which would hang the run on missing events). Instead the batch must FAIL cleanly, the
    // same as a thrown send, so the run terminates. (This guard is specific to the concurrent dispatcher, which is the
    // only path that invokes the id extractor; the synchronous K=1 path does not call it.)
    RecordingMessenger messenger = new RecordingMessenger();
    ControlledIndexer controlled = new ControlledIndexer(config(2), messenger);
    indexer = controlled;
    controlled.failIdExtraction("badDoc");
    messenger.queueDoc(Document.create("badDoc"));

    // The run must terminate (not hang) and must not throw: a bad field value is a document-level failure.
    controlled.run(1);

    assertEquals("the batch must FAIL rather than stall the run",
        List.of("FAIL:badDoc"), messenger.sentEvents());
    // The batch reached a defined conclusion (its document FAILed), so it is marked complete — a commit-on-completion
    // messenger commits its offset, so the unprocessable document is not re-delivered to fail again forever.
    assertEquals("the FAILed batch must still be marked complete", List.of("badDoc"), messenger.completedBatches());
    assertTrue("the send pool must be shut down", controlled.sendPoolTerminated());
  }

  @Test
  public void testThrowingIdExtractorStillCompletesEarlierBatchesInOrder() throws Exception {
    // A batch whose id extraction throws must not jump ahead of batches already in flight: completion stays in dispatch
    // order. doc1 is dispatched and held in flight; doc2's id extraction then throws. doc1 must complete (FINISH)
    // before doc2 is FAILed.
    RecordingMessenger messenger = new RecordingMessenger();
    indexer = new ControlledIndexer(config(2), messenger);
    indexer.blockSendsFor("doc1");
    indexer.failIdExtraction("doc2");
    queueDocsAndStartIndexer("doc1", "doc2");

    // doc1's send is in flight; the indexer thread then blocks in dispatch() handling doc2 (its failing extraction
    // drains the in-flight doc1 first, and doc1 is still held).
    awaitUntil(() -> indexer.hasStartedSend("doc1"));
    awaitNextBatchBlocked();
    // doc2 must not have been FAILed yet: it waits for the earlier doc1 to complete first.
    assertEquals(List.of(), messenger.sentEvents());

    // Releasing doc1 lets it finish and complete, then doc2 FAILs — in dispatch order.
    indexer.releaseSends("doc1");
    // Wait on completions, not events: a batch's events are sent before it is marked complete.
    awaitUntil(() -> messenger.completedBatches().size() == 2);
    assertEquals(List.of("FINISH:doc1", "FAIL:doc2"), messenger.sentEvents());
    // Both batches are marked complete, in dispatch order: doc1 after its successful send, doc2 after its FAIL.
    assertEquals(List.of("doc1", "doc2"), messenger.completedBatches());
  }

  @Test
  public void testThrowingBarrierCheckFailsBatchWithoutStalling() throws Exception {
    // The dispatcher also calls the barrier predicate (Indexer::isDeleteByQuery) before a batch is sent, and it too can
    // throw on a malformed field value. Like a throwing id extractor, it must not escape and stall the run: the batch
    // must FAIL cleanly. The deletion-marker config and fields make isDeleteByQuery evaluate through to the (throwing)
    // hasDeleteByFieldValues check.
    RecordingMessenger messenger = new RecordingMessenger();
    ControlledIndexer controlled = new ControlledIndexer(config(2, deleteByQuerySettings()), messenger);
    indexer = controlled;
    controlled.failBarrierCheck("badDoc");
    messenger.queueDoc(deleteByIdDoc("badDoc"));

    // The run must terminate (not hang) and must not throw.
    controlled.run(1);

    assertEquals("the batch must FAIL rather than stall the run",
        List.of("FAIL:badDoc"), messenger.sentEvents());
    assertEquals("the FAILed batch must still be marked complete", List.of("badDoc"), messenger.completedBatches());
    assertTrue("the send pool must be shut down", controlled.sendPoolTerminated());
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

  /**
   * An operation document for the ControlledIndexer's simulated destination: written under the destination id destId
   * (via idOverrideField "destId"), with the given op ("add", "delete") and, for an add, the version it carries.
   */
  private static Document opDoc(String docId, String destId, String op, String version) {
    Document doc = Document.create(docId);
    doc.setField("destId", destId);
    doc.setField("op", op);
    if (version != null) {
      doc.setField("version", version);
    }
    return doc;
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
    // Per document id, milliseconds its send sleeps (in addition to sendDelayMs), to make sends finish in a chosen order.
    private final Map<String, Long> perDocSendDelayMs = new ConcurrentHashMap<>();
    // A simulated destination, written by sendToIndex for documents carrying an "op" field: destination id -> the
    // "version" of the last add applied to it (absent once deleted). Guarded by itself. See applyToDestination.
    private final Map<String, String> destination = new HashMap<>();
    // Per destination id, the ids of the operation documents applied to it, in the order they were applied.
    private final Map<String, List<String>> appliedByDestId = new HashMap<>();
    // Per document id, a Throwable its send should throw (after any latch is released) instead of succeeding.
    private final Map<String, Throwable> sendFailures = new ConcurrentHashMap<>();
    // Document ids that sendToIndex reports as per-document failures (returned in the failed set, not thrown).
    private final Set<String> failedDocIds = ConcurrentHashMap.newKeySet();
    // Per document id, how many more send attempts should throw a retryable exception before succeeding.
    private final Map<String, AtomicInteger> retryableFailures = new ConcurrentHashMap<>();
    // Per document id, the total number of send attempts made (to assert that retries actually occurred).
    private final Map<String, AtomicInteger> sendAttempts = new ConcurrentHashMap<>();
    // Document ids for which id extraction (getDocIdOverride, reached via the dispatcher's destinationIdExtractor)
    // should throw, simulating a malformed field value that makes the extractor fail before the batch is sent.
    private final Set<String> idExtractionFailures = ConcurrentHashMap.newKeySet();
    // Document ids for which the barrier check (hasDeleteByFieldValues, reached via the dispatcher's isBarrierDocument
    // predicate) should throw, simulating a malformed field value that makes the predicate fail before the batch is sent.
    private final Set<String> barrierCheckFailures = ConcurrentHashMap.newKeySet();

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

    /** Makes the send of the given document id sleep this many milliseconds once it is released. */
    void setSendDelayMs(String docId, long delayMs) {
      perDocSendDelayMs.put(docId, delayMs);
    }

    /** A snapshot of the simulated destination: destination id to the version of its last applied add (deleted ids absent). */
    Map<String, String> destinationContent() {
      synchronized (destination) {
        return new HashMap<>(destination);
      }
    }

    /** The ids of the operation documents applied to the given destination id, in the order they were applied. */
    List<String> appliedOps(String destId) {
      synchronized (destination) {
        return new ArrayList<>(appliedByDestId.getOrDefault(destId, List.of()));
      }
    }

    /**
     * Applies the batch to the simulated destination, in order, the way a real backend would when the send executes. A
     * document participates only if it has an "op" field: "add" stores its "version" under its destination id, "delete"
     * removes that id, and "deleteByQuery" clears everything (a delete-by-query can match documents in any batch).
     */
    private void applyToDestination(List<Document> documents) {
      synchronized (destination) {
        for (Document doc : documents) {
          if (!doc.has("op")) {
            continue;
          }
          String override = getDocIdOverride(doc);
          String destId = override != null ? override : doc.getId();
          switch (doc.getString("op")) {
            case "add" -> destination.put(destId, doc.getString("version"));
            case "delete" -> destination.remove(destId);
            case "deleteByQuery" -> destination.clear();
            default -> throw new IllegalArgumentException("unknown op for " + doc.getId());
          }
          appliedByDestId.computeIfAbsent(destId, k -> new ArrayList<>()).add(doc.getId());
        }
      }
    }

    /** Makes sendToIndex report the given document id as a per-document failure (returned, not thrown). */
    void failDocument(String docId) {
      failedDocIds.add(docId);
    }

    /**
     * Makes id extraction throw for the given document id. The dispatcher calls the id extractor
     * ({@code Indexer::destinationIds}, which calls {@link #getDocIdOverride}) before a batch is sent, so this simulates
     * a malformed field value that makes that callback throw, and lets a test assert the batch still FAILs cleanly
     * rather than stalling the run.
     */
    void failIdExtraction(String docId) {
      idExtractionFailures.add(docId);
    }

    @Override
    protected String getDocIdOverride(Document doc) {
      if (idExtractionFailures.contains(doc.getId())) {
        throw new IllegalArgumentException("simulated id extraction failure for " + doc.getId());
      }
      return super.getDocIdOverride(doc);
    }

    /**
     * Makes the barrier check throw for the given document id. The dispatcher calls the barrier predicate
     * ({@code Indexer::isDeleteByQuery}, which calls {@link #hasDeleteByFieldValues}) before a batch is sent, so this
     * simulates a malformed field value that makes that callback throw, and lets a test assert the batch still FAILs
     * cleanly rather than stalling the run. (The test must supply deletion-marker config/fields so isDeleteByQuery
     * evaluates through to hasDeleteByFieldValues.)
     */
    void failBarrierCheck(String docId) {
      barrierCheckFailures.add(docId);
    }

    @Override
    protected boolean hasDeleteByFieldValues(Document doc) {
      if (barrierCheckFailures.contains(doc.getId())) {
        throw new IllegalArgumentException("simulated barrier check failure for " + doc.getId());
      }
      return super.hasDeleteByFieldValues(doc);
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
        long docDelayMs = perDocSendDelayMs.getOrDefault(docId, 0L);
        if (docDelayMs > 0) {
          Thread.sleep(docDelayMs);
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
        applyToDestination(documents);
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

    /** Adds a document to the in-memory FIFO queue, to be returned (in insertion order) by a later {@link #pollDocToIndex()}. */
    void queueDoc(Document document) {
      docsToIndex.add(document);
    }

    /** The number of times {@link #pollDocToIndex()} has been called. */
    int pollCount() {
      return pollCount.get();
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

    // Records batch completions in order and commits nothing at poll, so it is safe for concurrent batches.
    @Override
    public boolean commitsOnBatchCompletion() {
      return true;
    }
  }
}
