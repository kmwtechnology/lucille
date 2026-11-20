package com.kmwllc.lucille.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.kmwllc.lucille.util.LogUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KafkaConsumerMetricsTest {

  // a pipeline name no other test uses, so the metrics in the shared registry are this test's alone
  private static final String PIPELINE = "kafkaConsumerMetricsTest";
  private final MetricRegistry registry = SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG);

  @Before
  @After
  public void removeMetrics() {
    registry.removeMatching(MetricFilter.startsWith(PIPELINE + "."));
  }

  @Test
  public void testWorkerAndIndexerUseSeparateNames() {
    KafkaConsumerMetrics.forWorker(PIPELINE).commit(() -> { });
    assertEquals(1, registry.timer(PIPELINE + ".kafka.worker.commitTime").getCount());
    assertFalse(registry.getTimers().containsKey(PIPELINE + ".kafka.indexer.commitTime"));

    KafkaConsumerMetrics.forIndexer(PIPELINE).commit(() -> { });
    assertEquals(1, registry.timer(PIPELINE + ".kafka.indexer.commitTime").getCount());
    assertEquals(1, registry.timer(PIPELINE + ".kafka.worker.commitTime").getCount());
  }

  @Test
  public void testTimersComeFromTheSharedRegistry() {
    // an application may install its own registry under the shared name to supply its own Timer implementation, so the
    // timers must be the registry's, and shared by every consumer of the pipeline
    Timer registered = registry.timer(PIPELINE + KafkaConsumerMetrics.WORKER_POLL_TIME_SUFFIX);
    KafkaConsumerMetrics.forWorker(PIPELINE).timePoll().close();
    KafkaConsumerMetrics.forWorker(PIPELINE).timePoll().close();
    assertSame(registered, registry.timer(PIPELINE + KafkaConsumerMetrics.WORKER_POLL_TIME_SUFFIX));
    assertEquals(2, registered.getCount());
  }

  @Test
  public void testFailedCommitIsTimedCountedAndRethrown() {
    KafkaConsumerMetrics metrics = KafkaConsumerMetrics.forWorker(PIPELINE);
    IllegalStateException failure = new IllegalStateException("commit failed");

    IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> metrics.commit(() -> {
      throw failure;
    }));

    assertSame(failure, thrown);
    assertEquals(1, registry.timer(PIPELINE + KafkaConsumerMetrics.WORKER_COMMIT_TIME_SUFFIX).getCount());
    assertEquals(1, registry.meter(PIPELINE + KafkaConsumerMetrics.WORKER_COMMIT_FAILURES_SUFFIX).getCount());
  }

  @Test
  public void testSummaryIsEmptyAndRegistersNothingWithoutAConsumer() {
    assertEquals("", KafkaConsumerMetrics.workerSummary(PIPELINE));
    assertEquals("", KafkaConsumerMetrics.indexerSummary(PIPELINE));
    assertTrue(registry.getNames().stream().noneMatch(name -> name.startsWith(PIPELINE + ".")));
  }

  @Test
  public void testSummaryReportsPollsCommitsAndFailures() {
    KafkaConsumerMetrics metrics = KafkaConsumerMetrics.forIndexer(PIPELINE);
    metrics.timePoll().close();
    metrics.timePoll().close();
    metrics.commit(() -> { });
    assertThrows(IllegalStateException.class, () -> metrics.commit(() -> {
      throw new IllegalStateException();
    }));

    String summary = KafkaConsumerMetrics.indexerSummary(PIPELINE);
    assertTrue(summary, summary.startsWith("Kafka: 2 polls, mean "));
    assertTrue(summary, summary.contains("; 2 commits, mean "));
    assertTrue(summary, summary.endsWith(" ms, 1 failed."));
    assertEquals("", KafkaConsumerMetrics.workerSummary(PIPELINE));
  }
}
