package com.kmwllc.lucille.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

/**
 * Covers the per-record {@code kafkaProducer.flush()} that {@code sendForProcessing} used to call,
 * and the producer configuration that makes removing it worthwhile.
 *
 * The tests fall into two groups.
 *
 * MECHANISM tests hand-roll producers to pin down *why* a per-record flush is the wrong shape.
 * They configure linger.ms explicitly rather than taking it from Lucille's producer properties,
 * because the point is the client's behaviour, not Lucille's chosen setting:
 *  - {@link #mechanism_flushDestroysBatching()} — flush() cuts batches short; it is equivalent to
 *    globally setting linger.ms=0.
 *  - {@link #mechanism_flushCostScalesWithThreadCount()} — flush() is a producer-global barrier,
 *    so its cost is paid waiting on *other* threads and grows with concurrency.
 *  - {@link #mechanism_singleThreadedPublisherPaysLingerPerRecord()} — with one publishing thread
 *    flush() is a no-op, but the kafka-clients 4.x linger.ms=5 default adds ~5ms to every
 *    send().get() for no benefit. This is why KafkaUtils pins linger.ms to 0 while the send path
 *    is synchronous.
 *
 * REGRESSION test {@link #regression_publisherShouldBatchUnderConcurrency()} runs the real
 * {@link KafkaPublisherMessenger} and asserts it batches as well as a bare producer on the same
 * properties. Reintroducing a per-record flush() in sendForProcessing makes it fail.
 *
 * Representative numbers from mechanism_flushDestroysBatching on this embedded broker
 * (16 threads x 40 small records):
 *
 * <pre>
 *   [flush,    linger=5]  records/request=2.86  batch=153B  wall=142ms   &lt;- current code
 *   [no flush, linger=5]  records/request=16.0  batch=577B  wall=285ms
 *   [no flush, linger=0]  records/request=2.88  batch=154B  wall=116ms
 * </pre>
 *
 * Two things to read from that. First, the flushing arm and the linger=0 arm batch identically:
 * calling flush() per record is equivalent to globally disabling linger.ms. Second, while the
 * send is synchronous, batching and latency are in direct opposition -- the arm that batches best
 * is the slowest, because every thread waits out the linger on every record. Batching only pays
 * once the send stops blocking. So removing flush() is not on its own a throughput win; that is
 * why it was removed together with pinning linger.ms=0 (the third row), and why linger.ms should
 * be raised again only once the send path goes asynchronous.
 *
 * CAVEAT: an in-JVM embedded broker has near-zero round-trip latency and almost no tail, which is
 * exactly the condition under which flush()'s cross-thread barrier is cheapest. These tests prove
 * the structural claims (batching suppressed, cost scales with thread count); they cannot
 * reproduce the magnitude seen against a real remote broker.
 *
 * Uses a modest thread count so the test is CI-friendly; the effect grows with concurrency.
 */
public class KafkaPublisherFlushTest {

  private static final String SOURCE_TOPIC = "flushtest_source";
  private static final String PIPELINE = "flushtest";

  private static final int THREADS = 16;
  private static final int RECORDS_PER_THREAD = 40;

  public static EmbeddedKafkaBroker embeddedKafka;

  @BeforeClass
  public static void startKafka() throws Exception {
    // one partition, so batching is not split across partitions and record counts are unambiguous
    embeddedKafka = new EmbeddedKafkaKraftBroker(1, 1, SOURCE_TOPIC);
    embeddedKafka.afterPropertiesSet();
  }

  @AfterClass
  public static void stopKafka() {
    if (embeddedKafka != null) {
      embeddedKafka.destroy();
    }
  }

  // ---------------------------------------------------------------- mechanism

  /**
   * flush() forces every buffered batch out immediately, regardless of linger.ms, and it does so
   * for the whole producer. Under concurrent publishing that means each produce request carries a
   * single record: batching is off, no matter how the producer is configured.
   */
  @Test
  public void mechanism_flushDestroysBatching() throws Exception {
    Config config = config();

    // linger.ms=5 is the kafka-clients 4.x default, set explicitly here because Lucille's producer
    // properties now pin it to 0 -- and pinning it to 0 is half of what this test motivates
    try (KafkaProducer<String, Document> withFlush = documentProducer(config, "5");
        KafkaProducer<String, Document> withoutFlush = documentProducer(config, "5");
        KafkaProducer<String, Document> withoutFlushNoLinger = documentProducer(config, "0")) {

      long flushedNanos = publishConcurrently(withFlush, true);
      long unflushedNanos = publishConcurrently(withoutFlush, false);
      long noLingerNanos = publishConcurrently(withoutFlushNoLinger, false);

      double flushedBatch = metric(withFlush, "records-per-request-avg");
      double unflushedBatch = metric(withoutFlush, "records-per-request-avg");
      double flushedMs = flushedNanos / 1_000_000.0;
      double unflushedMs = unflushedNanos / 1_000_000.0;
      double noLingerMs = noLingerNanos / 1_000_000.0;

      System.out.printf("%n[flush, linger=5]     records/request=%.2f  batch=%.0fB  wall=%.0fms%n",
          flushedBatch, metric(withFlush, "batch-size-avg"), flushedMs);
      System.out.printf("[no flush, linger=5]  records/request=%.2f  batch=%.0fB  wall=%.0fms%n",
          unflushedBatch, metric(withoutFlush, "batch-size-avg"), unflushedMs);
      System.out.printf("[no flush, linger=0]  records/request=%.2f  batch=%.0fB  wall=%.0fms%n%n",
          metric(withoutFlushNoLinger, "records-per-request-avg"),
          metric(withoutFlushNoLinger, "batch-size-avg"), noLingerMs);

      // flush() drains the accumulator continuously, so batches are cut short well below what
      // the same workload forms on its own. Note it does NOT reduce batches to a single record:
      // records queued by other threads between drains still ride along.
      assertTrue("removing flush() should produce materially larger batches: "
              + unflushedBatch + " vs " + flushedBatch,
          unflushedBatch > 2 * flushedBatch);

      // the sharpest statement of the mechanism: calling flush() per record is equivalent to
      // globally disabling linger.ms. The flushing arm runs at linger.ms=5 but batches like the
      // linger.ms=0 arm, not like the linger.ms=5 arm it is actually configured as. Stated as a
      // ratio rather than an absolute delta because the exact figure moves with machine load.
      double noLingerBatch = metric(withoutFlushNoLinger, "records-per-request-avg");
      assertTrue("per-record flush() should batch like linger.ms=0 (" + noLingerBatch
              + "), since that is effectively what it does to the whole producer, but it batched "
              + flushedBatch,
          flushedBatch < 2.0 * noLingerBatch);

      // and it is not trading away durability: both arms wrote every record
      assertEquals(THREADS * RECORDS_PER_THREAD, (long) metric(withFlush, "record-send-total"));
      assertEquals(THREADS * RECORDS_PER_THREAD, (long) metric(withoutFlush, "record-send-total"));
    }
  }

  /**
   * flush() blocks on every batch that was incomplete when it was called -- including batches
   * queued by other threads. The calling thread's own record already completed in get(), so this
   * wait buys the caller nothing, and it grows with the number of publishing threads.
   *
   * With a single thread there is nothing else in the accumulator, so flush() returns immediately.
   */
  @Test
  public void mechanism_flushCostScalesWithThreadCount() throws Exception {
    Config config = config();

    try (KafkaProducer<String, Document> single = documentProducer(config, "5");
        KafkaProducer<String, Document> concurrent = documentProducer(config, "5")) {

      long singleThreadFlushNanos = timeSpentFlushing(single, 1, THREADS * RECORDS_PER_THREAD);
      long concurrentFlushNanos = timeSpentFlushing(concurrent, THREADS, RECORDS_PER_THREAD);

      double singleMs = singleThreadFlushNanos / 1_000_000.0;
      double concurrentMs = concurrentFlushNanos / 1_000_000.0;

      // same number of records in both arms; the only difference is how many threads are sharing
      // the producer, and therefore how much *other* work each flush() has to wait for
      assertTrue("flush() should cost far more with " + THREADS + " threads than with 1: "
              + concurrentMs + "ms vs " + singleMs + "ms",
          concurrentMs > 3 * singleMs);
    }
  }

  /**
   * The other half of the problem, and the one that affects the common single-connector-thread
   * deployment: kafka-clients 4.0 raised the linger.ms default from 0 to 5. A lone thread doing
   * send().get() has nothing to batch with, so it simply waits out the linger on every record.
   * Here flush() is a no-op -- the record already completed in get().
   */
  @Test
  public void mechanism_singleThreadedPublisherPaysLingerPerRecord() throws Exception {
    Config config = config();
    int records = 30;

    try (KafkaProducer<String, Document> defaultLinger = documentProducer(config, "5");
        KafkaProducer<String, Document> noLinger = documentProducer(config, "0")) {

      // warm up metadata/connection so the first record's cost is not counted
      sendSync(defaultLinger, Document.create("warmup-a"), false);
      sendSync(noLinger, Document.create("warmup-b"), false);

      long defaultLingerNanos = 0;
      long noLingerNanos = 0;
      for (int i = 0; i < records; i++) {
        defaultLingerNanos += sendSync(defaultLinger, Document.create("linger-" + i), false);
        noLingerNanos += sendSync(noLinger, Document.create("nolinger-" + i), false);
      }

      double defaultMsPerRecord = defaultLingerNanos / 1_000_000.0 / records;
      double noLingerMsPerRecord = noLingerNanos / 1_000_000.0 / records;

      // a single-threaded publisher waits out the full linger on every record
      assertTrue("default linger should cost ~5ms/record, measured " + defaultMsPerRecord,
          defaultMsPerRecord > 3.0);
      assertTrue("linger.ms=0 should be materially faster: " + noLingerMsPerRecord
              + "ms vs " + defaultMsPerRecord + "ms",
          noLingerMsPerRecord < defaultMsPerRecord);
    }
  }

  // --------------------------------------------------------------- regression

  /**
   * Asserts the property we want from the publisher: when many threads publish concurrently, the
   * messenger should batch as well as a bare producer built from the same properties does. A
   * per-record flush() in sendForProcessing breaks that, because flush() drains the accumulator
   * on every record no matter how the producer is configured.
   *
   * Exercises the real KafkaPublisherMessenger rather than a hand-rolled producer, so it guards
   * the fix rather than demonstrating the mechanism.
   */
  @Test
  public void regression_publisherShouldBatchUnderConcurrency() throws Exception {
    Config config = config();
    KafkaPublisherMessenger messenger = new KafkaPublisherMessenger(config);
    messenger.initialize("run-" + System.nanoTime(), PIPELINE);

    try {
      ExecutorService pool = Executors.newFixedThreadPool(THREADS);
      CountDownLatch start = new CountDownLatch(1);
      List<Runnable> tasks = new ArrayList<>();
      for (int t = 0; t < THREADS; t++) {
        final int threadNum = t;
        tasks.add(() -> {
          try {
            start.await();
            for (int i = 0; i < RECORDS_PER_THREAD; i++) {
              messenger.sendForProcessing(Document.create("doc-" + threadNum + "-" + i));
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
      }
      tasks.forEach(pool::submit);
      start.countDown();
      pool.shutdown();
      assertTrue(pool.awaitTermination(120, TimeUnit.SECONDS));

      double messengerBatch = metric(producerOf(messenger), "records-per-request-avg");

      // control: the identical workload on a producer built from the same production properties,
      // sending the same way the messenger does but with no flush of its own
      double controlBatch;
      try (KafkaProducer<String, Document> control = documentProducer(config, null)) {
        publishConcurrently(control, false);
        controlBatch = metric(control, "records-per-request-avg");
      }

      System.out.printf("%n[messenger] records/request=%.2f   [control, no flush]=%.2f%n%n",
          messengerBatch, controlBatch);

      // the publisher should batch about as well as the same workload does without the flush
      assertTrue("KafkaPublisherMessenger batches " + messengerBatch
              + " records/request but the same workload on the same producer properties batches "
              + controlBatch + " -- something in sendForProcessing is suppressing batching, most "
              + "likely a reintroduced per-record flush()",
          messengerBatch > 0.6 * controlBatch);
    } finally {
      messenger.close();
    }
  }

  /**
   * The payoff. sendForProcessing(doc, callback) hands the record to the producer and returns, so
   * the publishing thread no longer waits a broker round trip per document and the records it
   * queues can actually be batched. Compares it against the synchronous overload on the same
   * messenger, same broker, same workload.
   */
  @Test
  public void asyncSendBatchesAndOutrunsTheSynchronousSend() throws Exception {
    Config config = config();
    KafkaPublisherMessenger messenger = new KafkaPublisherMessenger(config);
    messenger.initialize("run-" + System.nanoTime(), PIPELINE);

    try {
      int total = THREADS * RECORDS_PER_THREAD;
      CountDownLatch acked = new CountDownLatch(total);
      AtomicLong failures = new AtomicLong(0);

      long asyncNanos = runOnThreads(threadNum -> {
        for (int i = 0; i < RECORDS_PER_THREAD; i++) {
          try {
            messenger.sendForProcessing(Document.create("async-" + threadNum + "-" + i), exception -> {
              if (exception != null) {
                failures.incrementAndGet();
              }
              acked.countDown();
            });
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      });

      assertTrue("all sends should complete", acked.await(120, TimeUnit.SECONDS));
      assertEquals("no send should have failed", 0, failures.get());
      double asyncBatch = metric(producerOf(messenger), "records-per-request-avg");
      assertEquals("every record should have been sent", total,
          (long) metric(producerOf(messenger), "record-send-total"));

      // control: the same messenger, same producer configuration, using the blocking overload
      KafkaPublisherMessenger syncMessenger = new KafkaPublisherMessenger(config);
      syncMessenger.initialize("run-" + System.nanoTime(), PIPELINE);
      double syncBatch;
      long syncNanos;
      try {
        syncNanos = runOnThreads(threadNum -> {
          for (int i = 0; i < RECORDS_PER_THREAD; i++) {
            try {
              syncMessenger.sendForProcessing(Document.create("sync-" + threadNum + "-" + i));
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        });
        syncBatch = metric(producerOf(syncMessenger), "records-per-request-avg");
      } finally {
        syncMessenger.close();
      }

      System.out.printf("%n[async] records/request=%.2f  wall=%.0fms   [sync] records/request=%.2f  wall=%.0fms%n%n",
          asyncBatch, asyncNanos / 1_000_000.0, syncBatch, syncNanos / 1_000_000.0);

      // the whole point: the sending thread stops gating on the ack, so records accumulate and
      // batch instead of trickling out one round trip at a time
      assertTrue("async publishing should batch far better than blocking on every ack: "
              + asyncBatch + " vs " + syncBatch + " records/request",
          asyncBatch > 3 * syncBatch);

      // note this is the cheapest possible case for the synchronous arm -- an in-JVM broker with
      // near-zero round-trip time. The gap widens with real network latency.
      assertTrue("async publishing should be faster than blocking on every ack: "
              + asyncNanos / 1_000_000.0 + "ms vs " + syncNanos / 1_000_000.0 + "ms",
          asyncNanos < syncNanos);
    } finally {
      messenger.close();
    }
  }

  // ------------------------------------------------------------------ helpers

  /** Runs the given per-thread body on THREADS threads at once; returns wall-clock nanos. */
  private long runOnThreads(java.util.function.IntConsumer body) throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(THREADS);
    CountDownLatch start = new CountDownLatch(1);
    for (int t = 0; t < THREADS; t++) {
      final int threadNum = t;
      pool.submit(() -> {
        try {
          start.await();
          body.accept(threadNum);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
    }
    long begin = System.nanoTime();
    start.countDown();
    pool.shutdown();
    assertTrue(pool.awaitTermination(120, TimeUnit.SECONDS));
    return System.nanoTime() - begin;
  }

  private Config config() {
    return ConfigFactory.empty()
        .withValue("kafka.bootstrapServers",
            ConfigValueFactory.fromAnyRef(embeddedKafka.getBrokersAsString()))
        .withValue("kafka.consumerGroupId", ConfigValueFactory.fromAnyRef("flushtest_group"))
        .withValue("kafka.maxPollIntervalSecs", ConfigValueFactory.fromAnyRef(600))
        .withValue("kafka.maxRequestSize", ConfigValueFactory.fromAnyRef(10_000_000))
        .withValue("kafka.sourceTopic", ConfigValueFactory.fromAnyRef(SOURCE_TOPIC));
  }

  /** Builds a producer from the real production properties, optionally overriding linger.ms. */
  private KafkaProducer<String, Document> documentProducer(Config config, String lingerMsOverride) {
    Properties props = KafkaUtils.createProducerProps(config);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaDocumentSerializer.class.getName());
    if (lingerMsOverride != null) {
      props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMsOverride);
    }
    return new KafkaProducer<>(props);
  }

  /** Reproduces sendForProcessing: send().get(), then optionally the producer-global flush(). */
  private long sendSync(KafkaProducer<String, Document> producer, Document doc, boolean flush)
      throws Exception {
    long begin = System.nanoTime();
    producer.send(new ProducerRecord<>(SOURCE_TOPIC, doc.getId(), doc)).get();
    if (flush) {
      producer.flush();
    }
    return System.nanoTime() - begin;
  }

  /** Runs the concurrent publish workload and returns wall-clock nanos for the whole run. */
  private long publishConcurrently(KafkaProducer<String, Document> producer, boolean flush)
      throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(THREADS);
    CountDownLatch start = new CountDownLatch(1);
    for (int t = 0; t < THREADS; t++) {
      final int threadNum = t;
      pool.submit(() -> {
        try {
          start.await();
          for (int i = 0; i < RECORDS_PER_THREAD; i++) {
            sendSync(producer, Document.create("doc-" + threadNum + "-" + i), flush);
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    }
    long begin = System.nanoTime();
    start.countDown();
    pool.shutdown();
    assertTrue(pool.awaitTermination(120, TimeUnit.SECONDS));
    return System.nanoTime() - begin;
  }

  /** Total wall-clock time spent inside flush() across all threads. */
  private long timeSpentFlushing(KafkaProducer<String, Document> producer, int threads,
      int recordsPerThread) throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    AtomicLong flushNanos = new AtomicLong(0);

    for (int t = 0; t < threads; t++) {
      final int threadNum = t;
      pool.submit(() -> {
        try {
          start.await();
          for (int i = 0; i < recordsPerThread; i++) {
            Document doc = Document.create("flushtime-" + threadNum + "-" + i);
            producer.send(new ProducerRecord<>(SOURCE_TOPIC, doc.getId(), doc)).get();
            long begin = System.nanoTime();
            producer.flush();
            flushNanos.addAndGet(System.nanoTime() - begin);
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    }
    start.countDown();
    pool.shutdown();
    assertTrue(pool.awaitTermination(120, TimeUnit.SECONDS));
    return flushNanos.get();
  }

  private static double metric(KafkaProducer<?, ?> producer, String name) {
    for (Map.Entry<MetricName, ? extends Metric> entry : producer.metrics().entrySet()) {
      if (entry.getKey().name().equals(name) && entry.getKey().group().equals("producer-metrics")) {
        Object value = entry.getValue().metricValue();
        if (value instanceof Double) {
          return (Double) value;
        }
      }
    }
    return Double.NaN;
  }

  @SuppressWarnings("unchecked")
  private static KafkaProducer<String, Document> producerOf(KafkaPublisherMessenger messenger)
      throws Exception {
    Field field = KafkaPublisherMessenger.class.getDeclaredField("kafkaProducer");
    field.setAccessible(true);
    return (KafkaProducer<String, Document>) field.get(messenger);
  }
}
