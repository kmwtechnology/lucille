package com.kmwllc.lucille.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.KafkaDocument;
import com.kmwllc.lucille.util.LogUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Tests the poll and commit metrics of the standalone Worker's messenger, using a MockConsumer. */
public class KafkaWorkerMessengerMetricsTest {

  // a pipeline name no other test uses, so the metrics in the shared registry are this test's alone
  private static final String PIPELINE = "kafkaWorkerMessengerMetricsTest";
  private static final TopicPartition P0 = new TopicPartition(PIPELINE + "_source", 0);

  private final MetricRegistry registry = SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG);
  private volatile RuntimeException commitException;
  private MockConsumer<String, KafkaDocument> consumer;
  private KafkaWorkerMessenger messenger;

  @Before
  public void setUp() {
    removeMetrics();
    consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
      @Override
      public synchronized void commitSync() {
        if (commitException != null) {
          throw commitException;
        }
        super.commitSync();
      }
    };
    consumer.setMaxPollRecords(1);
    Config config = ConfigFactory.parseString("kafka { bootstrapServers: \"localhost:9092\", consumerGroupId: \"group\", "
        + "maxRequestSize: 1048576, events: false }");
    messenger = new KafkaWorkerMessenger(config, PIPELINE, consumer);
    consumer.rebalance(Set.of(P0));
    consumer.updateBeginningOffsets(Map.of(P0, 0L));
  }

  @After
  public void tearDown() throws Exception {
    messenger.close();
    removeMetrics();
  }

  private void removeMetrics() {
    registry.removeMatching(MetricFilter.startsWith(PIPELINE + "."));
  }

  @Test
  public void testPollsAreTimed() throws Exception {
    KafkaDocument value = new KafkaDocument((ObjectNode) new ObjectMapper().readTree(Document.create("doc0", "run1").toString()));
    consumer.addRecord(new ConsumerRecord<>(P0.topic(), 0, 0L, "doc0", value));

    assertNotNull(messenger.pollDocToProcess());
    assertNull(messenger.pollDocToProcess());

    assertEquals(2, registry.timer(PIPELINE + KafkaConsumerMetrics.WORKER_POLL_TIME_SUFFIX).getCount());
    // the indexer's names are not used by the worker
    assertFalse(registry.getTimers().containsKey(PIPELINE + KafkaConsumerMetrics.INDEXER_POLL_TIME_SUFFIX));
  }

  private void addRecord(long offset) throws Exception {
    String id = "doc" + offset;
    KafkaDocument value = new KafkaDocument((ObjectNode) new ObjectMapper().readTree(Document.create(id, "run1").toString()));
    consumer.addRecord(new ConsumerRecord<>(P0.topic(), 0, offset, id, value));
  }

  private long commits() {
    return registry.timer(PIPELINE + KafkaConsumerMetrics.WORKER_COMMIT_TIME_SUFFIX).getCount();
  }

  private long commitFailures() {
    return registry.meter(PIPELINE + KafkaConsumerMetrics.WORKER_COMMIT_FAILURES_SUFFIX).getCount();
  }

  @Test
  public void testCommitsAreTimedAndFailuresCountedAndRethrown() throws Exception {
    addRecord(0);
    assertNotNull(messenger.pollDocToProcess());
    messenger.commitPendingDocOffsets();
    assertEquals(1, commits());
    assertEquals(0, commitFailures());

    addRecord(1);
    assertNotNull(messenger.pollDocToProcess());
    CommitFailedException failure = new CommitFailedException();
    commitException = failure;
    assertSame(failure, assertThrows(CommitFailedException.class, () -> messenger.commitPendingDocOffsets()));
    assertEquals(2, commits());
    assertEquals(1, commitFailures());
  }

  @Test
  public void testIdleWorkerDoesNotCommit() throws Exception {
    // the Worker commits after every empty poll; with nothing polled there is nothing to commit
    assertNull(messenger.pollDocToProcess());
    messenger.commitPendingDocOffsets();
    assertNull(messenger.pollDocToProcess());
    messenger.commitPendingDocOffsets();
    assertEquals(0, commits());
    assertTrue(consumer.committed(Set.of(P0)).isEmpty() || consumer.committed(Set.of(P0)).get(P0) == null);

    // once a record is polled, its offset is committed, once
    addRecord(0);
    assertNotNull(messenger.pollDocToProcess());
    messenger.commitPendingDocOffsets();
    assertEquals(1, commits());
    assertEquals(1L, consumer.committed(Set.of(P0)).get(P0).offset());
    assertNull(messenger.pollDocToProcess());
    messenger.commitPendingDocOffsets();
    assertEquals(1, commits());
  }

  @Test
  public void testFailedCommitIsRetriedOnTheNextEmptyPoll() throws Exception {
    addRecord(0);
    assertNotNull(messenger.pollDocToProcess());
    commitException = new CommitFailedException();
    assertThrows(CommitFailedException.class, () -> messenger.commitPendingDocOffsets());

    commitException = null;
    assertNull(messenger.pollDocToProcess());
    messenger.commitPendingDocOffsets();
    assertEquals(2, commits());
    assertEquals(1, commitFailures());
    assertEquals(1L, consumer.committed(Set.of(P0)).get(P0).offset());
  }
}
