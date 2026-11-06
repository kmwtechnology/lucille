package com.kmwllc.lucille.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.util.LogUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class HybridWorkerMessengerTest {

  private final TopicPartition a0 = new TopicPartition("a", 0);
  private final TopicPartition a1 = new TopicPartition("a", 1);
  private final TopicPartition b0 = new TopicPartition("b", 0);

  // a pipeline name no other test uses, so the metrics in the shared registry are this test's alone
  private static final String PIPELINE = "hybridWorkerMessengerTest";
  private final MetricRegistry metrics = SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG);

  private KafkaConsumer consumer;
  private LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets;
  private LinkedBlockingQueue<Document> pipelineDest;
  private HybridWorkerMessenger messenger;

  @Before
  public void setUp() {
    removeMetrics();
    consumer = mock(KafkaConsumer.class);
    offsets = new LinkedBlockingQueue<>();
    pipelineDest = new LinkedBlockingQueue<>(1);
    messenger = new HybridWorkerMessenger(ConfigFactory.parseString("kafka.events: false"), PIPELINE, pipelineDest,
        offsets, consumer);
  }

  @After
  public void tearDown() {
    removeMetrics();
  }

  private void removeMetrics() {
    SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG).removeMatching(MetricFilter.startsWith(PIPELINE + "."));
  }

  @Test
  public void testEmptyQueueDoesNotCommit() throws Exception {
    messenger.commitPendingDocOffsets();
    verify(consumer, never()).commitSync(anyMap());
  }

  @Test
  public void testSingleMapCommittedAsIs() throws Exception {
    Map<TopicPartition, OffsetAndMetadata> m = Map.of(a0, new OffsetAndMetadata(5));
    offsets.add(m);
    messenger.commitPendingDocOffsets();
    verify(consumer, times(1)).commitSync(m);
    assertTrue(offsets.isEmpty());
  }

  @Test
  public void testSamePartitionOutOfOrderKeepsMax() throws Exception {
    offsets.add(Map.of(a0, new OffsetAndMetadata(10)));
    offsets.add(Map.of(a0, new OffsetAndMetadata(4)));
    offsets.add(Map.of(a0, new OffsetAndMetadata(7)));
    messenger.commitPendingDocOffsets();

    ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> captor = ArgumentCaptor.forClass(Map.class);
    verify(consumer, times(1)).commitSync(captor.capture());
    assertEquals(Map.of(a0, new OffsetAndMetadata(10)), captor.getValue());
  }

  @Test
  public void testMultiplePartitionsAndTopics() throws Exception {
    offsets.add(Map.of(a0, new OffsetAndMetadata(3), a1, new OffsetAndMetadata(9)));
    offsets.add(Map.of(a0, new OffsetAndMetadata(8), b0, new OffsetAndMetadata(2)));
    offsets.add(Map.of(a1, new OffsetAndMetadata(1), b0, new OffsetAndMetadata(6)));
    messenger.commitPendingDocOffsets();

    ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> captor = ArgumentCaptor.forClass(Map.class);
    verify(consumer, times(1)).commitSync(captor.capture());
    assertEquals(Map.of(a0, new OffsetAndMetadata(8), a1, new OffsetAndMetadata(9), b0, new OffsetAndMetadata(6)),
        captor.getValue());
  }

  @Test
  public void testPollIsTimed() throws Exception {
    when(consumer.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());
    messenger.pollDocToProcess();
    messenger.pollDocToProcess();
    assertEquals(2, metrics.timer(PIPELINE + HybridWorkerMessenger.POLL_TIME_SUFFIX).getCount());
  }

  @Test
  public void testPollIsTimedWhenItThrows() throws Exception {
    when(consumer.poll(any(Duration.class))).thenThrow(new WakeupException());
    assertThrows(WakeupException.class, () -> messenger.pollDocToProcess());
    assertEquals(1, metrics.timer(PIPELINE + HybridWorkerMessenger.POLL_TIME_SUFFIX).getCount());
  }

  @Test
  public void testCommitIsTimedOnlyWhenThereIsSomethingToCommit() throws Exception {
    messenger.commitPendingDocOffsets();
    assertEquals(0, metrics.timer(PIPELINE + HybridWorkerMessenger.COMMIT_TIME_SUFFIX).getCount());

    offsets.add(Map.of(a0, new OffsetAndMetadata(5)));
    offsets.add(Map.of(a0, new OffsetAndMetadata(6)));
    messenger.commitPendingDocOffsets();
    assertEquals(1, metrics.timer(PIPELINE + HybridWorkerMessenger.COMMIT_TIME_SUFFIX).getCount());
    assertEquals(0, metrics.meter(PIPELINE + HybridWorkerMessenger.COMMIT_FAILURES_SUFFIX).getCount());
  }

  @Test
  public void testCommitFailureIsCountedAndRethrown() throws Exception {
    CommitFailedException failure = new CommitFailedException();
    doThrow(failure).when(consumer).commitSync(anyMap());
    offsets.add(Map.of(a0, new OffsetAndMetadata(5)));

    CommitFailedException thrown = assertThrows(CommitFailedException.class, () -> messenger.commitPendingDocOffsets());
    assertSame(failure, thrown);
    assertEquals(1, metrics.meter(PIPELINE + HybridWorkerMessenger.COMMIT_FAILURES_SUFFIX).getCount());
    assertEquals(1, metrics.timer(PIPELINE + HybridWorkerMessenger.COMMIT_TIME_SUFFIX).getCount());
  }

  @Test
  public void testSendWithRoomInQueueIsNotCountedAsFull() throws Exception {
    Document doc = Document.create("doc1");
    messenger.sendForIndexing(doc);
    assertSame(doc, pipelineDest.peek());
    assertEquals(0, metrics.meter(PIPELINE + HybridWorkerMessenger.INDEXER_QUEUE_FULL_SUFFIX).getCount());
  }

  @Test
  public void testSendToFullQueueIsCountedAndWaitsForSpace() throws Exception {
    Document first = Document.create("doc1");
    Document second = Document.create("doc2");
    messenger.sendForIndexing(first);

    Thread sender = new Thread(() -> {
      try {
        messenger.sendForIndexing(second);
      } catch (MessengerException e) {
        throw new RuntimeException(e);
      }
    });
    sender.start();

    // the second send finds the queue full, is counted, and blocks until the first document is taken
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (metrics.meter(PIPELINE + HybridWorkerMessenger.INDEXER_QUEUE_FULL_SUFFIX).getCount() == 0
        && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertEquals(1, metrics.meter(PIPELINE + HybridWorkerMessenger.INDEXER_QUEUE_FULL_SUFFIX).getCount());
    assertTrue(sender.isAlive());

    assertSame(first, pipelineDest.take());
    sender.join(10_000);
    assertFalse(sender.isAlive());
    assertSame(second, pipelineDest.poll());
  }
}
