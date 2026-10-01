package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.indexer.CSVIndexer;
import com.kmwllc.lucille.message.HybridIndexerMessenger;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Test;

/**
 * Tests indexer.maxConcurrentBatches: ordered completion, the dispatch gates (capacity, ID overlap, delete-by-query
 * barrier), retries, shutdown draining, and error propagation. Each batch's send blocks on a latch keyed by the "tag" of
 * its first document, so the tests control the order in which sends finish.
 */
public class IndexerConcurrencyTest {

  private static final long TIMEOUT_MS = 10000;
  private static final AtomicInteger RUN_IDS = new AtomicInteger();

  private Thread indexerThread;
  private ControlledIndexer indexer;

  @After
  public void tearDown() throws Exception {
    if (indexer != null) {
      indexer.releaseAll();
      indexer.terminate();
    }
    if (indexerThread != null) {
      indexerThread.join(TIMEOUT_MS);
    }
  }

  @Test
  public void testCompletionFollowsDispatchOrder() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    start(new ControlledIndexer(config(2), messenger), "a", "b");
    indexer.gate("a", "b");
    awaitTrue(() -> indexer.started.containsAll(List.of("a", "b")));

    indexer.release("b");
    awaitTrue(() -> indexer.finished.contains("b"));
    awaitPollCycle(messenger.polls);
    assertEquals("b finished first but must not complete ahead of a", List.of(), messenger.completed());

    indexer.release("a");
    awaitTrue(() -> messenger.completed().size() == 2);
    assertEquals(List.of("a", "b"), messenger.completed());
    assertEquals(List.of("FINISH:a", "FINISH:b"), messenger.events());
    assertEquals(2, indexer.maxConcurrent.get());
  }

  @Test
  public void testCapacityLimitsBatchesInFlight() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    ControlledIndexer controlled = new ControlledIndexer(config(2), messenger);
    controlled.gate("a", "b", "c");
    start(controlled, "a", "b", "c", "d", "e");
    awaitTrue(() -> indexer.started.containsAll(List.of("a", "b")));
    awaitIndexerWaitingForBatch();
    assertFalse(indexer.started.contains("c"));
    // The indexer thread is blocked dispatching c (d sits in its partly filled batch), so it stops polling: e is still
    // waiting in the messenger's queue.
    assertEquals(1, messenger.queue.size());

    indexer.release("a");
    awaitTrue(() -> indexer.started.contains("c"));
    indexer.release("b", "c");
    awaitTrue(() -> messenger.completed().size() == 5);
    assertEquals(List.of("a", "b", "c", "d", "e"), messenger.completed());
    assertEquals(2, indexer.maxConcurrent.get());
  }

  @Test
  public void testOverlappingIdWaitsButDisjointBatchDoesNot() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    ControlledIndexer controlled = new ControlledIndexer(config(3), messenger);
    controlled.gate("a1", "c", "a2");
    indexer = controlled;
    // a1 and a2 share the id "a"; c is disjoint.
    startWith(controlled, messenger, doc("a", "a1"), doc("c", "c"), doc("a", "a2"));

    awaitTrue(() -> indexer.started.containsAll(List.of("a1", "c")));
    awaitIndexerWaitingForBatch();
    assertFalse("a2 must wait for a1, which writes the same id", indexer.started.contains("a2"));

    indexer.release("a1");
    awaitTrue(() -> indexer.started.contains("a2"));
    indexer.release("c", "a2");
    awaitTrue(() -> messenger.completed().size() == 3);
    assertEquals(List.of("a1", "c", "a2"), messenger.completed());
  }

  @Test
  public void testIdOverrideFieldDeterminesOverlap() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    Map<String, Object> extra = Map.of("indexer.idOverrideField", "destId");
    ControlledIndexer controlled = new ControlledIndexer(config(3, extra), messenger);
    controlled.gate("x", "y");
    indexer = controlled;
    Document x = doc("x", "x");
    x.setField("destId", "same");
    Document y = doc("y", "y");
    y.setField("destId", "same");
    startWith(controlled, messenger, x, y);

    awaitTrue(() -> indexer.started.contains("x"));
    awaitIndexerWaitingForBatch();
    assertFalse("y is written under the same destination id as x", indexer.started.contains("y"));
    indexer.release("x");
    awaitTrue(() -> indexer.started.contains("y"));
  }

  @Test
  public void testChildIdsDetermineOverlap() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    ControlledIndexer controlled = new ControlledIndexer(config(3), messenger);
    controlled.gate("p1", "q", "p2");
    indexer = controlled;
    // p1 and p2 both write the child "shared"; q writes nothing in common.
    Document p1 = doc("p1", "p1");
    p1.addChild(Document.create("shared"));
    Document p2 = doc("p2", "p2");
    p2.addChild(Document.create("shared"));
    startWith(controlled, messenger, p1, doc("q", "q"), p2);

    awaitTrue(() -> indexer.started.containsAll(List.of("p1", "q")));
    awaitIndexerWaitingForBatch();
    assertFalse("p2 writes the child id shared, which p1 also writes", indexer.started.contains("p2"));

    indexer.release("p1");
    awaitTrue(() -> indexer.started.contains("p2"));
    indexer.release("q", "p2");
    awaitTrue(() -> messenger.completed().size() == 3);
    assertEquals(List.of("p1", "q", "p2"), messenger.completed());
  }

  @Test
  public void testGrandchildIdDeterminesOverlap() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    ControlledIndexer controlled = new ControlledIndexer(config(4), messenger);
    controlled.gate("p1", "p2");
    indexer = controlled;
    Document p1 = doc("p1", "p1");
    Document child = Document.create("child");
    child.addChild(Document.create("deep"));
    p1.addChild(child);
    startWith(controlled, messenger, p1, doc("deep", "p2"));

    awaitTrue(() -> indexer.started.contains("p1"));
    awaitIndexerWaitingForBatch();
    assertFalse("p2 is written under p1's grandchild id", indexer.started.contains("p2"));
    indexer.release("p1");
    awaitTrue(() -> indexer.started.contains("p2"));
  }

  @Test
  public void testDeleteByQueryIsABarrier() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    Map<String, Object> deletion = Map.of(
        "indexer.deletionMarkerField", "deleted", "indexer.deletionMarkerFieldValue", "true",
        "indexer.deleteByFieldField", "dbqField", "indexer.deleteByFieldValue", "dbqValue");
    ControlledIndexer controlled = new ControlledIndexer(config(3, deletion), messenger);
    controlled.gate("x", "d", "y");
    indexer = controlled;
    Document d = doc("d", "d");
    d.setField("deleted", "true");
    d.setField("dbqField", "category");
    d.setField("dbqValue", "obsolete");
    startWith(controlled, messenger, doc("x", "x"), d, doc("y", "y"));

    awaitTrue(() -> indexer.started.contains("x"));
    awaitIndexerWaitingForBatch();
    assertFalse("delete-by-query must wait for zero in flight", indexer.started.contains("d"));

    indexer.release("x");
    awaitTrue(() -> indexer.started.contains("d"));
    // d has been dispatched, so the indexer thread is now waiting for d itself.
    awaitIndexerWaitingForBatch();
    assertFalse("nothing may be dispatched behind a delete-by-query", indexer.started.contains("y"));

    indexer.release("d");
    awaitTrue(() -> indexer.started.contains("y"));
    indexer.release("y");
    awaitTrue(() -> messenger.completed().size() == 3);
    assertEquals(List.of("x", "d", "y"), messenger.completed());
    assertEquals(1, indexer.maxConcurrent.get());
  }

  @Test
  public void testDeleteByIdIsNotABarrier() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    Map<String, Object> deletion = Map.of(
        "indexer.deletionMarkerField", "deleted", "indexer.deletionMarkerFieldValue", "true",
        "indexer.deleteByFieldField", "dbqField", "indexer.deleteByFieldValue", "dbqValue");
    ControlledIndexer controlled = new ControlledIndexer(config(3, deletion), messenger);
    controlled.gate("x", "d");
    indexer = controlled;
    Document d = doc("d", "d");
    d.setField("deleted", "true");
    startWith(controlled, messenger, doc("x", "x"), d);

    awaitTrue(() -> indexer.started.containsAll(List.of("x", "d")));
  }

  @Test
  public void testRetryRunsOnPoolWithoutReorderingCompletion() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    Map<String, Object> retry = Map.<String, Object>of("indexer.maxRetries", 2, "indexer.retryWaitDurationMs", 1);
    ControlledIndexer controlled = new ControlledIndexer(config(2, retry), messenger);
    controlled.failNextAttempt("a", new IndexerRetryableException(503, "unavailable", null));
    controlled.gate("a");
    start(controlled, "a", "b");

    awaitTrue(() -> indexer.finished.contains("b"));
    awaitPollCycle(messenger.polls);
    assertEquals(List.of(), messenger.completed());

    indexer.release("a");
    awaitTrue(() -> messenger.completed().size() == 2);
    assertEquals(List.of("a", "b"), messenger.completed());
    assertEquals(2, indexer.attempts("a"));
    assertEquals(List.of("FINISH:a", "FINISH:b"), messenger.events());
  }

  @Test
  public void testFailedBatchKeepsOrder() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    ControlledIndexer controlled = new ControlledIndexer(config(2), messenger);
    controlled.failNextAttempt("b", new IndexerException("bad request"));
    controlled.gate("a");
    start(controlled, "a", "b");

    awaitTrue(() -> indexer.finished.contains("b"));
    indexer.release("a");
    awaitTrue(() -> messenger.completed().size() == 2);
    assertEquals(List.of("a", "b"), messenger.completed());
    assertEquals(List.of("FINISH:a", "FAIL:b"), messenger.events());
  }

  @Test
  public void testPerDocumentFailuresAreReported() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    ControlledIndexer controlled = new ControlledIndexer(config(2, Map.<String, Object>of("indexer.batchSize", 2)), messenger);
    controlled.failDocs.add("a2");
    start(controlled, "a1", "a2");
    awaitTrue(() -> messenger.completed().size() == 1);
    assertEquals(List.of("FAIL:a2", "FINISH:a1"), messenger.events());
  }

  @Test
  public void testTerminateDrainsInFlightBatches() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    ControlledIndexer controlled = new ControlledIndexer(config(3), messenger);
    controlled.gate("a", "b", "c");
    start(controlled, "a", "b", "c");
    awaitTrue(() -> indexer.started.containsAll(List.of("a", "b", "c")));

    indexer.terminate();
    // run() is draining: it waits for a rather than returning with batches in flight.
    awaitIndexerWaitingForBatch();
    assertTrue("run() must not return with batches in flight", indexerThread.isAlive());

    indexer.release("c", "b", "a");
    indexerThread.join(TIMEOUT_MS);
    assertFalse(indexerThread.isAlive());
    assertEquals(List.of("a", "b", "c"), messenger.completed());
    assertTrue(messenger.closed);
    assertFalse("send pool threads must stop with the indexer", sendThreadsAlive(controlled));
  }

  @Test
  public void testRunIterationsDrainsInFlightBatches() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    ControlledIndexer controlled = new ControlledIndexer(config(4), messenger);
    for (String tag : List.of("a", "b", "c", "d", "e")) {
      messenger.queue.add(doc(tag, tag));
    }
    controlled.run(5);
    assertEquals(List.of("a", "b", "c", "d", "e"), messenger.completed());
    assertFalse(sendThreadsAlive(controlled));
  }

  @Test
  public void testErrorInSendSurfacesOnIndexerThread() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    ControlledIndexer controlled = new ControlledIndexer(config(2), messenger);
    controlled.failNextAttempt("a", new StackOverflowError("boom"));
    messenger.queue.add(doc("a", "a"));

    StackOverflowError error = assertThrows(StackOverflowError.class, () -> controlled.run(1));
    assertEquals("boom", error.getMessage());
    // batchComplete still runs for the failed batch, as it does for synchronous sends
    assertEquals(List.of("a"), messenger.completed());
    assertTrue(messenger.closed);
    assertFalse(sendThreadsAlive(controlled));
  }

  @Test
  public void testErrorWithOtherBatchesInFlightStopsPool() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    ControlledIndexer controlled = new ControlledIndexer(config(2), messenger);
    controlled.failNextAttempt("a", new StackOverflowError("boom"));
    controlled.gate("a", "b");
    messenger.queue.add(doc("a", "a"));
    messenger.queue.add(doc("b", "b"));
    AtomicReference<Throwable> thrown = new AtomicReference<>();
    Thread runner = new Thread(() -> {
      try {
        controlled.run(2);
      } catch (Throwable t) {
        thrown.set(t);
      }
    });
    runner.start();
    awaitTrue(() -> controlled.started.containsAll(List.of("a", "b")));

    // a's Error surfaces while b is still blocked in its send; the pool is shut down with b abandoned.
    controlled.release("a");
    runner.join(TIMEOUT_MS);
    assertFalse(runner.isAlive());
    assertTrue(String.valueOf(thrown.get()), thrown.get() instanceof StackOverflowError);
    assertEquals(List.of("a"), messenger.completed());
    assertTrue(messenger.closed);
    awaitTrue(() -> !sendThreadsAlive(controlled));
  }

  @Test
  public void testInterruptWhileWaitingStillCompletesInFlightBatches() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    ControlledIndexer controlled = new ControlledIndexer(config(2), messenger);
    controlled.gate("a");
    start(controlled, "a", "b", "c", "d");
    // With a and b in flight, the indexer thread blocks dispatching c until a completes.
    awaitTrue(() -> indexer.started.containsAll(List.of("a", "b")));
    awaitIndexerWaitingForBatch();

    indexerThread.interrupt();
    // The wait absorbs the interrupt (clearing the flag) and goes back to waiting for a.
    awaitTrue(() -> !indexerThread.isInterrupted());
    awaitIndexerWaitingForBatch();
    assertFalse("an interrupt must not abandon an in-flight batch", indexer.started.contains("c"));

    indexer.release("a");
    indexerThread.join(TIMEOUT_MS);
    assertFalse(indexerThread.isAlive());
    // The restored interrupt makes the next poll fail, which terminates the indexer after it drains what it holds.
    assertEquals(List.of("a", "b", "c"), messenger.completed().subList(0, 3));
  }

  /**
   * An interrupt that arrives while the indexer waits for an in-flight batch must not reach the messenger calls that
   * complete it: HybridIndexerMessenger.batchComplete uses a blocking put, which would throw at once and lose the offset.
   */
  @Test
  public void testInterruptWhileWaitingStillQueuesOffsets() throws Exception {
    Config config = config(2).withFallback(ConfigFactory.parseMap(Map.of("kafka.events", false)));
    LinkedBlockingQueue<Document> dest = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets = new LinkedBlockingQueue<>();
    HybridIndexerMessenger messenger = new HybridIndexerMessenger(config, dest, offsets, null, "pipeline1");
    ControlledIndexer controlled = new ControlledIndexer(config, messenger);
    controlled.gate("k0");
    addKafkaDocs(dest, 3);
    indexer = controlled;
    indexerThread = new Thread(controlled);
    indexerThread.start();
    // k0 and k1 are in flight; the indexer thread waits for k0 before it can dispatch k2.
    awaitTrue(() -> controlled.started.containsAll(List.of("k0", "k1")));
    awaitIndexerWaitingForBatch();

    indexerThread.interrupt();
    awaitTrue(() -> !indexerThread.isInterrupted());
    controlled.release("k0");
    indexerThread.join(TIMEOUT_MS);
    assertFalse(indexerThread.isAlive());

    List<Long> committed = new ArrayList<>();
    Map<TopicPartition, OffsetAndMetadata> next;
    while ((next = offsets.poll()) != null) {
      committed.add(next.get(new TopicPartition("source", 0)).offset());
    }
    assertEquals(List.of(1L, 2L, 3L), committed);
  }

  @Test
  public void testKeepAliveWhileWaitingForBatch() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    ControlledIndexer controlled = new ControlledIndexer(config(2), messenger);
    controlled.gate("a");
    start(controlled, "a", "b", "c");
    awaitTrue(() -> indexer.started.containsAll(List.of("a", "b")));
    awaitIndexerWaitingForBatch();

    awaitTrue(() -> messenger.keepAlives.get() >= 3);
    assertFalse(indexer.started.contains("c"));
    indexer.release("a");
    awaitTrue(() -> messenger.completed().size() == 3);
  }

  @Test
  public void testKeepAliveFailureDoesNotStopWaiting() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    messenger.keepAliveFailure = new RuntimeException("keepAlive failed");
    ControlledIndexer controlled = new ControlledIndexer(config(2), messenger);
    controlled.gate("a");
    start(controlled, "a", "b", "c");
    awaitTrue(() -> messenger.keepAlives.get() >= 2);
    assertTrue(indexerThread.isAlive());
    indexer.release("a");
    awaitTrue(() -> messenger.completed().size() == 3);
    assertEquals(List.of("a", "b", "c"), messenger.completed());
  }

  @Test
  public void testNoKeepAliveWithoutBatchesInFlight() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    start(new ControlledIndexer(config(2), messenger), "a", "b");
    awaitTrue(() -> messenger.completed().size() == 2);
    // Idle polling for longer than the keep-alive interval.
    Thread.sleep(Indexer.KEEP_ALIVE_INTERVAL_MS * 3 / 2);
    assertEquals(0, messenger.keepAlives.get());
  }

  @Test
  public void testNoKeepAliveForSynchronousSends() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    ControlledIndexer controlled = new ControlledIndexer(config(1), messenger);
    controlled.gate("a");
    start(controlled, "a");
    awaitTrue(() -> indexer.started.contains("a"));
    Thread.sleep(Indexer.KEEP_ALIVE_INTERVAL_MS * 3 / 2);
    indexer.release("a");
    awaitTrue(() -> messenger.completed().size() == 1);
    assertEquals(0, messenger.keepAlives.get());
  }

  @Test
  public void testSendsRunOnLucilleNamedPoolThreads() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    ControlledIndexer controlled = new ControlledIndexer(config(2), messenger);
    messenger.queue.add(doc("a", "a"));
    controlled.run(1);
    String name = controlled.threadNames.get(0);
    assertTrue(name, name.startsWith("Lucille-" + controlled.runId + "-IndexerSend-"));
  }

  @Test
  public void testSendThreadNamesAreUniqueAcrossIndexers() throws Exception {
    // Two indexers of the same run, as with several worker/indexer pairs.
    String runId = nextRunId();
    List<String> names = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      RecordingMessenger messenger = new RecordingMessenger();
      ControlledIndexer controlled = new ControlledIndexer(config(2), messenger, runId);
      messenger.queue.add(doc("a", "a"));
      controlled.run(1);
      names.add(controlled.threadNames.get(0));
    }
    assertTrue(names.toString(), names.stream().allMatch(n -> n.startsWith("Lucille-" + runId + "-IndexerSend-")));
    assertNotEquals(names.get(0), names.get(1));
  }

  @Test
  public void testStringMaxConcurrentBatches() throws Exception {
    // An environment substitution always yields a string, which spec validation accepts as a number.
    Config fromEnv = ConfigFactory.parseString("indexer { batchSize: 1, maxConcurrentBatches: ${?K} }")
        .resolveWith(ConfigFactory.parseMap(Map.of("K", "4")));
    assertEquals(4, Indexer.getMaxConcurrentBatches(fromEnv));
    Config quoted = ConfigFactory.parseString("indexer { batchSize: \"1\", maxConcurrentBatches: \"3\" }");
    assertEquals(3, Indexer.getMaxConcurrentBatches(quoted));

    RecordingMessenger messenger = new RecordingMessenger();
    ControlledIndexer controlled = new ControlledIndexer(fromEnv, messenger);
    messenger.queue.add(doc("a", "a"));
    controlled.run(1);
    assertEquals(List.of("a"), messenger.completed());
    assertTrue(controlled.threadNames.get(0), controlled.threadNames.get(0).contains("IndexerSend"));
  }

  @Test
  public void testSingleBatchSendsOnIndexerThread() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    ControlledIndexer controlled = new ControlledIndexer(config(1), messenger);
    messenger.queue.add(doc("a", "a"));
    controlled.run(1);
    assertEquals(Thread.currentThread().getName(), controlled.threadNames.get(0));
    assertEquals(List.of("a"), messenger.completed());
  }

  @Test
  public void testInvalidMaxConcurrentBatches() {
    assertThrows(IllegalArgumentException.class, () -> new ControlledIndexer(config(0), new RecordingMessenger()));
    assertThrows(IllegalArgumentException.class, () -> new ControlledIndexer(config(-2), new RecordingMessenger()));
  }

  @Test
  public void testIndexerWithoutConcurrentSupportRejectsConcurrency() {
    Config csvConfig = ConfigFactory.parseMap(Map.of(
        "indexer.type", "csv", "indexer.maxConcurrentBatches", 2,
        "csv.columns", List.of("id"), "csv.path", "target/IndexerConcurrencyTest.csv"));
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> new CSVIndexer(csvConfig, new RecordingMessenger(), false, "testing"));
    assertTrue(e.getMessage(), e.getMessage().contains("does not support concurrent sends"));
  }

  @Test
  public void testIndexerWithoutConcurrentSupportAcceptsOne() {
    Config csvConfig = ConfigFactory.parseMap(Map.of(
        "indexer.type", "csv", "indexer.maxConcurrentBatches", 1,
        "csv.columns", List.of("id"), "csv.path", "target/IndexerConcurrencyTest.csv"));
    new CSVIndexer(csvConfig, new RecordingMessenger(), false, "testing").closeConnection();
  }

  /**
   * Uses the real HybridIndexerMessenger, which turns each completed batch into a per-partition offset to commit. An
   * offset may only be queued once every earlier batch on that partition has completed.
   */
  @Test
  public void testHybridOffsetsNeverCoverIncompleteBatch() throws Exception {
    Config config = config(3).withFallback(ConfigFactory.parseMap(Map.of("kafka.events", false)));
    LinkedBlockingQueue<Document> dest = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets = new LinkedBlockingQueue<>();
    AtomicInteger polls = new AtomicInteger();
    HybridIndexerMessenger messenger = new HybridIndexerMessenger(config, dest, offsets, null, "pipeline1") {
      @Override
      public Document pollDocToIndex() throws Exception {
        polls.incrementAndGet();
        return super.pollDocToIndex();
      }
    };
    ControlledIndexer controlled = new ControlledIndexer(config, messenger);
    controlled.gate("k0", "k1", "k2");
    addKafkaDocs(dest, 3);
    indexer = controlled;
    indexerThread = new Thread(controlled);
    indexerThread.start();
    awaitTrue(() -> controlled.started.containsAll(List.of("k0", "k1", "k2")));

    controlled.release("k2", "k1");
    awaitTrue(() -> controlled.finished.containsAll(List.of("k1", "k2")));
    awaitPollCycle(polls);
    assertTrue("k1 and k2 finished, but k0 is incomplete: nothing may be committed", offsets.isEmpty());

    controlled.release("k0");
    TopicPartition partition = new TopicPartition("source", 0);
    List<Long> committed = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      committed.add(offsets.poll(TIMEOUT_MS, TimeUnit.MILLISECONDS).get(partition).offset());
    }
    assertEquals(List.of(1L, 2L, 3L), committed);
  }

  // A send slower than indexer.batchTimeout must not make the next add flush the leftover document as a batch of its
  // own: time the indexer thread spends blocked on sends does not count toward the timeout.
  @Test
  public void testSlowSynchronousSendDoesNotSplitBatches() throws Exception {
    assertFullBatchesDespiteSlowSends(1);
  }

  // The same for time spent waiting for a free slot when maxConcurrentBatches is reached.
  @Test
  public void testWaitForFreeSlotDoesNotSplitBatches() throws Exception {
    assertFullBatchesDespiteSlowSends(2);
  }

  private void assertFullBatchesDespiteSlowSends(int maxConcurrentBatches) throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    ControlledIndexer controlled = new ControlledIndexer(
        config(maxConcurrentBatches, Map.of("indexer.batchSize", 2, "indexer.batchTimeout", 50)), messenger);
    controlled.sendDelayMs = 200;
    for (String tag : List.of("a", "b", "c", "d", "e", "f", "g", "h")) {
      messenger.queue.add(doc(tag, tag));
    }
    controlled.run(8);
    assertEquals(List.of("a,b", "c,d", "e,f", "g,h"), messenger.completed());
  }

  // --- helpers ---

  private static Config config(int maxConcurrentBatches) {
    return config(maxConcurrentBatches, Map.of());
  }

  private static Config config(int maxConcurrentBatches, Map<String, Object> extra) {
    Map<String, Object> settings = new HashMap<>(Map.of(
        "indexer.batchSize", 1, "indexer.batchTimeout", 20, "indexer.maxConcurrentBatches", maxConcurrentBatches));
    settings.putAll(extra);
    return ConfigFactory.parseMap(settings);
  }

  private static Document doc(String id, String tag) {
    Document doc = Document.create(id);
    doc.setField("tag", tag);
    return doc;
  }

  private void start(ControlledIndexer controlled, String... tags) {
    Document[] docs = new Document[tags.length];
    for (int i = 0; i < tags.length; i++) {
      docs[i] = doc(tags[i], tags[i]);
    }
    startWith(controlled, (RecordingMessenger) controlled.messenger, docs);
  }

  private static void addKafkaDocs(LinkedBlockingQueue<Document> dest, int count) throws Exception {
    for (int i = 0; i < count; i++) {
      dest.add(new KafkaDocument(
          new ConsumerRecord<>("source", 0, i, "k" + i, "{\"id\":\"k" + i + "\",\"tag\":\"k" + i + "\"}")));
    }
  }

  private void startWith(ControlledIndexer controlled, RecordingMessenger messenger, Document... docs) {
    Collections.addAll(messenger.queue, docs);
    indexer = controlled;
    indexerThread = new Thread(controlled);
    indexerThread.start();
  }

  private static void awaitTrue(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.currentTimeMillis() + TIMEOUT_MS;
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("Condition not met within " + TIMEOUT_MS + " ms");
      }
      Thread.sleep(5);
    }
  }

  /**
   * Waits until the indexer thread is blocked waiting for an in-flight batch's send to finish. From then until that send
   * finishes, the indexer provably dispatches nothing more, so asserting that a batch has not started is meaningful.
   */
  private void awaitIndexerWaitingForBatch() throws InterruptedException {
    awaitTrue(() -> isWaitingForBatch(indexerThread));
  }

  private static boolean isWaitingForBatch(Thread thread) {
    if (!isParked(thread)) {
      return false;
    }
    boolean inAwaitOutcome = false;
    for (StackTraceElement frame : thread.getStackTrace()) {
      if (frame.getClassName().equals(Indexer.class.getName()) && frame.getMethodName().equals("awaitOutcome")) {
        inAwaitOutcome = true;
        break;
      }
    }
    // Check the state again, so the stack was sampled while the thread was parked.
    return inAwaitOutcome && isParked(thread);
  }

  private static boolean isParked(Thread thread) {
    Thread.State state = thread.getState();
    return state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING;
  }

  /**
   * Waits until the indexer has started two more polls. The first of them began after this call, and the second begins
   * only once the first poll's cycle, including its completion of finished batches, is over.
   */
  private static void awaitPollCycle(AtomicInteger polls) throws InterruptedException {
    int start = polls.get();
    awaitTrue(() -> polls.get() >= start + 2);
  }

  private static String nextRunId() {
    return "IndexerConcurrencyTest" + RUN_IDS.incrementAndGet();
  }

  // Whether any send pool thread of the given indexer's run is still alive.
  private static boolean sendThreadsAlive(ControlledIndexer controlled) {
    String prefix = "Lucille-" + controlled.runId + "-IndexerSend";
    return Thread.getAllStackTraces().keySet().stream()
        .anyMatch(t -> t.isAlive() && t.getName().startsWith(prefix));
  }

  private static String tag(Document doc) {
    return doc.has("tag") ? doc.getString("tag") : doc.getId();
  }

  /**
   * An Indexer whose sends block on per-tag latches and can be scripted to fail. Records the order in which sends start
   * and finish and the highest number of concurrent sends.
   */
  static class ControlledIndexer extends Indexer {

    public static final Spec SPEC = SpecBuilder.indexer().build();

    final IndexerMessenger messenger;
    final List<String> started = Collections.synchronizedList(new ArrayList<>());
    final List<String> finished = Collections.synchronizedList(new ArrayList<>());
    final List<String> threadNames = Collections.synchronizedList(new ArrayList<>());
    final Set<String> failDocs = ConcurrentHashMap.newKeySet();
    final AtomicInteger maxConcurrent = new AtomicInteger();
    private final AtomicInteger concurrent = new AtomicInteger();
    private final Map<String, CountDownLatch> gates = new ConcurrentHashMap<>();
    private final Map<String, Deque<Throwable>> failures = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();

    // When positive, every send sleeps this long after its gate opens.
    volatile long sendDelayMs;

    // Unique per test, so a test can find its own send pool threads.
    final String runId;

    ControlledIndexer(Config config, IndexerMessenger messenger) {
      this(config, messenger, nextRunId());
    }

    ControlledIndexer(Config config, IndexerMessenger messenger, String runId) {
      super(config, messenger, false, "IndexerConcurrencyTest", runId);
      this.messenger = messenger;
      this.runId = runId;
    }

    void gate(String... tags) {
      for (String tag : tags) {
        gates.put(tag, new CountDownLatch(1));
      }
    }

    void release(String... tags) {
      for (String tag : tags) {
        gates.get(tag).countDown();
      }
    }

    void releaseAll() {
      gates.values().forEach(CountDownLatch::countDown);
    }

    void failNextAttempt(String tag, Throwable t) {
      failures.computeIfAbsent(tag, k -> new ArrayDeque<>()).add(t);
    }

    int attempts(String tag) {
      return attempts.get(tag).get();
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
      String tag = tag(documents.get(0));
      attempts.computeIfAbsent(tag, k -> new AtomicInteger()).incrementAndGet();
      threadNames.add(Thread.currentThread().getName());
      int now = concurrent.incrementAndGet();
      maxConcurrent.accumulateAndGet(now, Math::max);
      if (!started.contains(tag)) {
        started.add(tag);
      }
      try {
        CountDownLatch gate = gates.get(tag);
        if (gate != null && !gate.await(TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
          throw new IllegalStateException("gate for " + tag + " was never released");
        }
        if (sendDelayMs > 0) {
          Thread.sleep(sendDelayMs);
        }
        Deque<Throwable> pending = failures.get(tag);
        Throwable failure = pending == null ? null : pending.poll();
        if (failure instanceof Error error) {
          throw error;
        } else if (failure != null) {
          throw (Exception) failure;
        }
        return documents.stream()
            .filter(d -> failDocs.contains(tag(d)))
            .map(d -> Pair.<Document, Exception>of(d, new IndexerException("rejected " + tag(d))))
            .collect(Collectors.toSet());
      } finally {
        concurrent.decrementAndGet();
        if (!finished.contains(tag)) {
          finished.add(tag);
        }
      }
    }
  }

  /** An IndexerMessenger backed by an in-memory queue that records events and batch completions in order. */
  static class RecordingMessenger implements IndexerMessenger {

    final LinkedBlockingQueue<Document> queue = new LinkedBlockingQueue<>();
    final AtomicInteger polls = new AtomicInteger();
    final AtomicInteger keepAlives = new AtomicInteger();
    volatile RuntimeException keepAliveFailure;
    private final List<String> events = Collections.synchronizedList(new ArrayList<>());
    private final List<String> completed = Collections.synchronizedList(new ArrayList<>());
    volatile boolean closed;

    List<String> events() {
      return new ArrayList<>(events);
    }

    List<String> completed() {
      return new ArrayList<>(completed);
    }

    @Override
    public Document pollDocToIndex() throws Exception {
      polls.incrementAndGet();
      return queue.poll(10, TimeUnit.MILLISECONDS);
    }

    @Override
    public void keepAlive() {
      keepAlives.incrementAndGet();
      if (keepAliveFailure != null) {
        throw keepAliveFailure;
      }
    }

    @Override
    public void sendEvent(Event event) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void sendEvent(Document document, String message, Event.Type type) {
      events.add(type + ":" + tag(document));
    }

    @Override
    public void sendEvents(List<Document> documents, String message, Event.Type type) {
      for (Document document : documents) {
        events.add(type + ":" + tag(document));
      }
    }

    @Override
    public void close() {
      closed = true;
    }

    @Override
    public void batchComplete(List<Document> batch) {
      completed.add(batch.stream().map(IndexerConcurrencyTest::tag).collect(Collectors.joining(",")));
    }
  }
}
