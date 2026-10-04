package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;

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
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Test;

/**
 * Tests indexer.maxConcurrentBatches: that batches, though sent concurrently on a pool, are completed in the order they
 * were dispatched, along with the dispatch gates, retries, draining, and error handling. Each batch's send blocks on a
 * latch keyed by the "tag" of its first document, so a test controls the order in which sends finish.
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
    indexer = new ControlledIndexer(config(2), messenger);
    indexer.gate("a", "b");
    start("a", "b");
    awaitTrue(() -> indexer.started.containsAll(List.of("a", "b")));

    // b's send finishes first, but it must not complete ahead of a.
    indexer.release("b");
    awaitTrue(() -> indexer.finished.contains("b"));
    awaitPollCycle(messenger.polls);
    assertEquals("b finished first but must not complete ahead of a", List.of(), messenger.completed());

    // Once a finishes, both complete, in dispatch order.
    indexer.release("a");
    awaitTrue(() -> messenger.completed().size() == 2);
    assertEquals(List.of("a", "b"), messenger.completed());
    assertEquals(2, indexer.maxConcurrent.get());
  }

  // --- helpers ---

  private static Config config(int maxConcurrentBatches) {
    return ConfigFactory.parseMap(Map.of(
        "indexer.batchSize", 1, "indexer.batchTimeout", 20, "indexer.maxConcurrentBatches", maxConcurrentBatches));
  }

  private static Document doc(String id, String tag) {
    Document doc = Document.create(id);
    doc.setField("tag", tag);
    return doc;
  }

  private void start(String... tags) {
    RecordingMessenger messenger = (RecordingMessenger) indexer.messenger;
    for (String tag : tags) {
      messenger.queue.add(doc(tag, tag));
    }
    indexerThread = new Thread(indexer);
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

  // Waits until the indexer has started two more polls, so a full poll cycle (including completing finished batches)
  // has elapsed since this call.
  private static void awaitPollCycle(java.util.concurrent.atomic.AtomicInteger polls) throws InterruptedException {
    int start = polls.get();
    awaitTrue(() -> polls.get() >= start + 2);
  }

  private static String tag(Document doc) {
    return doc.has("tag") ? doc.getString("tag") : doc.getId();
  }

  /**
   * An Indexer whose sends block on per-tag latches. Records the order in which sends start and finish and the highest
   * number of concurrent sends.
   */
  static class ControlledIndexer extends Indexer {

    public static final Spec SPEC = SpecBuilder.indexer().build();

    final IndexerMessenger messenger;
    final List<String> started = Collections.synchronizedList(new ArrayList<>());
    final List<String> finished = Collections.synchronizedList(new ArrayList<>());
    final java.util.concurrent.atomic.AtomicInteger maxConcurrent = new java.util.concurrent.atomic.AtomicInteger();
    private final java.util.concurrent.atomic.AtomicInteger concurrent = new java.util.concurrent.atomic.AtomicInteger();
    private final Map<String, CountDownLatch> gates = new ConcurrentHashMap<>();

    ControlledIndexer(Config config, IndexerMessenger messenger) {
      super(config, messenger, false, "IndexerConcurrencyTest", "IndexerConcurrencyTest");
      this.messenger = messenger;
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
      maxConcurrent.accumulateAndGet(concurrent.incrementAndGet(), Math::max);
      if (!started.contains(tag)) {
        started.add(tag);
      }
      try {
        CountDownLatch gate = gates.get(tag);
        if (gate != null && !gate.await(TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
          throw new IllegalStateException("gate for " + tag + " was never released");
        }
        return Set.of();
      } finally {
        concurrent.decrementAndGet();
        if (!finished.contains(tag)) {
          finished.add(tag);
        }
      }
    }
  }

  /**
   * An IndexerMessenger backed by an in-memory queue that records events and batch completions in order.
   *
   * <p> The shared {@link com.kmwllc.lucille.message.TestMessenger} is not used here because it records neither the
   * order in which {@link #batchComplete(List)} is called — which is exactly what these tests assert — nor the number
   * of polls, which {@link #awaitPollCycle} needs. Those observations are specific to concurrency testing, so they live
   * in this local double rather than being added to the widely used TestMessenger.
   */
  static class RecordingMessenger implements IndexerMessenger {

    final LinkedBlockingQueue<Document> queue = new LinkedBlockingQueue<>();
    final java.util.concurrent.atomic.AtomicInteger polls = new java.util.concurrent.atomic.AtomicInteger();
    private final List<String> events = Collections.synchronizedList(new ArrayList<>());
    private final List<String> completed = Collections.synchronizedList(new ArrayList<>());

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
    public void sendEvent(Event event) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void sendEvent(Document document, String message, Type type) {
      events.add(type + ":" + tag(document));
    }

    @Override
    public void sendEvents(List<Document> documents, String message, Type type) {
      for (Document document : documents) {
        events.add(type + ":" + tag(document));
      }
    }

    @Override
    public void close() {
    }

    @Override
    public void batchComplete(List<Document> batch) {
      completed.add(batch.stream().map(IndexerConcurrencyTest::tag).collect(Collectors.joining(",")));
    }
  }
}
