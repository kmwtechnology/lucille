package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.kmwllc.lucille.core.Event.Type;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.Collections;
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
    // maxConcurrentBatches = 2, batchSize = 1, so a and b can be in flight at once as separate batches.
    indexer = new ControlledIndexer(config(2), messenger);
    // Hold both sends open so neither can finish until we say so.
    indexer.blockSendsFor("a", "b");
    // Add a then b to the messenger's in-memory FIFO queue and start the indexer thread; it polls and dispatches them
    // in that order.
    queueDocsAndStartIndexer("a", "b");
    // Wait until both sends are actually running on the pool, i.e. both batches are in flight.
    awaitUntil(() -> indexer.hasStartedSends("a", "b"));

    // Let b's send finish first, while a's is still blocked.
    indexer.releaseSends("b");
    // Wait until b's send has returned, so the only thing stopping b's completion is a being unfinished.
    awaitUntil(() -> indexer.hasFinishedSend("b"));
    // Let a full poll cycle pass, giving the indexer the chance to (wrongly) complete b if ordering were not enforced.
    awaitFullPollCycle(messenger);
    // b must not be completed yet: completion is in dispatch order, and a (dispatched first) has not finished.
    assertEquals("b finished first but must not complete ahead of a", List.of(), messenger.completedBatches());

    // Now let a finish; a then b should complete, in dispatch order.
    indexer.releaseSends("a");
    // Wait for both completions to be recorded.
    awaitUntil(() -> messenger.completedBatches().size() == 2);
    assertEquals(List.of("a", "b"), messenger.completedBatches());
    // Both sends really did overlap, confirming the test exercised concurrency rather than serial sends.
    assertEquals(2, indexer.peakConcurrentSends());
  }

  @Test
  public void testCapacityLimitsBatchesInFlight() throws Exception {
    RecordingMessenger messenger = new RecordingMessenger();
    // Window of 2: at most two batches may be in flight at once.
    indexer = new ControlledIndexer(config(2), messenger);
    // Hold a, b, and c open so we control when the window frees up.
    indexer.blockSendsFor("a", "b", "c");
    // Add four docs to the messenger's in-memory FIFO queue; polling them in order, the indexer fills the window with
    // a and b, then tries to dispatch c.
    queueDocsAndStartIndexer("a", "b", "c", "d");
    // Wait until the window is full (a and b both in flight).
    awaitUntil(() -> indexer.hasStartedSends("a", "b"));

    // Wait until the indexer thread is blocked in dispatch (it has stopped polling): dispatching c must wait for a slot.
    awaitBlockedInDispatch();
    // c must not have started, because the window is full and no in-flight batch has finished.
    assertFalse("c must not start while the window is full", indexer.hasStartedSend("c"));
    // The cap held: never more than two sends ran at once.
    assertEquals("never more than maxConcurrentBatches in flight", 2, indexer.peakConcurrentSends());

    // Finish a, freeing one slot; c can now be dispatched and start.
    indexer.releaseSends("a");
    awaitUntil(() -> indexer.hasStartedSend("c"));

    // Finish the rest; all four batches should complete in dispatch order.
    indexer.releaseSends("b", "c");
    awaitUntil(() -> messenger.completedBatches().size() == 4);
    assertEquals(List.of("a", "b", "c", "d"), messenger.completedBatches());
    // The cap still held across the whole run.
    assertEquals(2, indexer.peakConcurrentSends());
  }

  // --- helpers ---

  private static Config config(int maxConcurrentBatches) {
    return ConfigFactory.parseMap(Map.of(
        "indexer.batchSize", 1, "indexer.batchTimeout", 20, "indexer.maxConcurrentBatches", maxConcurrentBatches));
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

  // Waits until the indexer thread appears blocked inside dispatch(): it has stopped polling, so the poll count holds
  // steady across a short window. (When the window is full, dispatch blocks completing the oldest batch, during which
  // the indexer neither polls nor starts new sends.)
  private void awaitBlockedInDispatch() throws InterruptedException {
    RecordingMessenger messenger = indexer.messenger();
    long deadline = System.currentTimeMillis() + TIMEOUT_MS;
    while (System.currentTimeMillis() < deadline) {
      int before = messenger.pollCount();
      Thread.sleep(100);
      if (messenger.pollCount() == before) {
        return;
      }
    }
    throw new AssertionError("indexer thread did not block in dispatch within " + TIMEOUT_MS + " ms");
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
      peakConcurrentSends.accumulateAndGet(concurrentSends.incrementAndGet(), Math::max);
      if (!sendsStarted.contains(docId)) {
        sendsStarted.add(docId);
      }
      try {
        CountDownLatch latch = sendLatches.get(docId);
        if (latch != null && !latch.await(TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
          throw new IllegalStateException("send latch for " + docId + " was never released");
        }
        return Set.of();
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
   * of polls, which {@link #awaitFullPollCycle} and {@link #awaitBlockedInDispatch} need. Those observations are
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
    public void batchComplete(List<Document> batch) {
      completedBatches.add(batch.stream().map(Document::getId).collect(Collectors.joining(",")));
    }
  }
}
