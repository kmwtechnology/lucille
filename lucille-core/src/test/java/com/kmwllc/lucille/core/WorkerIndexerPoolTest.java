package com.kmwllc.lucille.core;

import static com.kmwllc.lucille.util.ThreadNameUtils.areLucilleThreadsRunning;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.Test;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.kmwllc.lucille.message.HybridWorkerMessenger;
import com.kmwllc.lucille.message.KafkaConsumerMetrics;
import com.kmwllc.lucille.util.LogUtils;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests WorkerIndexerPool.
 *
 * Note: There is additional coverage in HybridKafkaTest.testWorkerIndexerPool();
 * that test is in a separate class because it depends on an embedded test environment
 */
@RunWith(JUnit4.class)
public class WorkerIndexerPoolTest {

  @Test
  public void testParseConfig() throws Exception {
    // reuse basic config from WorkerPoolTest
    Config config = ConfigFactory.load("WorkerPoolTest/config.conf");

    WorkerIndexerPool pool1 = new WorkerIndexerPool(config, "pipeline1", true, null);
    WorkerIndexerPool pool2 = new WorkerIndexerPool(config, "pipeline2", true, null);
    WorkerIndexerPool pool3 = new WorkerIndexerPool(config, "pipeline3", true, null);
    WorkerIndexerPool pool4 = new WorkerIndexerPool(ConfigFactory.empty(), "pipeline4", true, null);

    assertEquals(14, pool1.getNumWorkers());
    assertEquals(23, pool2.getNumWorkers());
    assertEquals(7, pool3.getNumWorkers());
    assertEquals(WorkerPool.DEFAULT_POOL_SIZE, pool4.getNumWorkers());
  }

  @Test
  public void testThreadCleanupUponEncounteringConfigProblem() throws Exception {
    // simulate a config error by passing an empty config to WorkerIndexerPool
    Config config = ConfigFactory.empty();
    WorkerIndexerPool pool = new WorkerIndexerPool(config, "pipeline12345", true, null);

    assertThrows(Exception.class, () -> { pool.start(); });

    assertFalse(areLucilleThreadsRunning());
  }

  @Test
  public void testMinRemainingCapacity() throws Exception {
    LinkedBlockingQueue<Document> roomy = new LinkedBlockingQueue<>(10);
    LinkedBlockingQueue<Document> tight = new LinkedBlockingQueue<>(3);
    tight.add(Document.create("doc1"));
    tight.add(Document.create("doc2"));

    assertEquals(1, WorkerIndexerPool.minRemainingCapacity(List.of(roomy, tight)));

    tight.add(Document.create("doc3"));
    assertEquals(0, WorkerIndexerPool.minRemainingCapacity(List.of(roomy, tight)));

    assertEquals(10, WorkerIndexerPool.minRemainingCapacity(Arrays.asList(roomy, null)));
    assertEquals(0, WorkerIndexerPool.minRemainingCapacity(List.of()));
  }

  @Test
  public void testStatusMessageIncludesKafkaAndQueueMetricsOnceRegistered() throws Exception {
    String pipeline = "workerIndexerPoolStatusTest";
    MetricRegistry registry = SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG);
    registry.removeMatching(MetricFilter.startsWith(pipeline + "."));
    try {
      WorkerIndexerPool pool = new WorkerIndexerPool(ConfigFactory.empty(), pipeline, true, null);
      // before any messenger exists, only the pipeline's own numbers
      assertEquals("0 docs processed. One minute rate: 0.00 docs/sec. Mean pipeline latency: 0.00 ms/doc.",
          pool.statusMessage(true));

      // what a HybridWorkerMessenger for the pipeline registers
      KafkaConsumerMetrics.forWorker(pipeline);
      registry.meter(pipeline + HybridWorkerMessenger.INDEXER_QUEUE_FULL_SUFFIX).mark(3);

      String periodic = pool.statusMessage(true);
      assertTrue(periodic, periodic.contains(" Kafka: 0 polls, mean 0.00 ms; 0 commits, mean 0.00 ms, 0 failed."));
      assertTrue(periodic, periodic.endsWith(" Indexer queue: 3 docs waited for space, lowest remaining capacity 0."));
      String onStop = pool.statusMessage(false);
      assertTrue(onStop, onStop.endsWith(" Indexer queue: 3 docs waited for space."));
    } finally {
      registry.removeMatching(MetricFilter.startsWith(pipeline + "."));
    }
  }
}
