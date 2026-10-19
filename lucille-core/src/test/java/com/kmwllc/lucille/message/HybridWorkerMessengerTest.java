package com.kmwllc.lucille.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.KafkaDocument;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class HybridWorkerMessengerTest {

  private final TopicPartition a0 = new TopicPartition("a", 0);
  private final TopicPartition a1 = new TopicPartition("a", 1);
  private final TopicPartition b0 = new TopicPartition("b", 0);

  private KafkaConsumer consumer;
  private LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets;
  private HybridWorkerMessenger messenger;

  @Before
  public void setUp() {
    consumer = mock(KafkaConsumer.class);
    when(consumer.assignment()).thenReturn(Set.of(a0, a1, b0));
    offsets = new LinkedBlockingQueue<>();
    messenger = createMessenger(10_000);
  }

  private HybridWorkerMessenger createMessenger(long drainTimeoutMs) {
    Config config = ConfigFactory.parseString("kafka.events: false\nkafka.rebalanceDrainTimeoutMs: " + drainTimeoutMs);
    return new HybridWorkerMessenger(config, "pipeline1", new LinkedBlockingQueue<Document>(), offsets, consumer);
  }

  private static KafkaDocument kafkaDoc(TopicPartition partition, long offset) throws Exception {
    String id = partition + "@" + offset;
    return new KafkaDocument(
        new ConsumerRecord<>(partition.topic(), partition.partition(), offset, id, Document.create(id).toString()));
  }

  private static void reportLater(LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> queue,
      Map<TopicPartition, OffsetAndMetadata> report, long delayMs) {
    Thread reporter = new Thread(() -> {
      try {
        Thread.sleep(delayMs);
        queue.put(report);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    reporter.setDaemon(true);
    reporter.start();
  }

  @Test(timeout = 10000)
  public void testRevokeWaitsForIndexerThenCommits() throws Exception {
    messenger.sendForIndexing(kafkaDoc(a0, 9));
    messenger.sendForIndexing(kafkaDoc(a1, 4));
    messenger.sendForIndexing(Document.create("child-without-kafka-metadata"));
    // the indexer reports one batch before the revocation, and the rest while the worker is waiting for it
    offsets.add(Map.of(a0, new OffsetAndMetadata(10)));
    reportLater(offsets, Map.of(a1, new OffsetAndMetadata(5)), 300);

    long start = System.nanoTime();
    messenger.onPartitionsRevoked(List.of(a0, a1));
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

    verify(consumer, times(1)).commitSync(Map.of(a0, new OffsetAndMetadata(10), a1, new OffsetAndMetadata(5)));
    assertTrue("revocation should have waited for the indexer, took " + elapsedMs + " ms", elapsedMs >= 250);
  }

  @Test(timeout = 10000)
  public void testRevokeGivesUpAfterDrainTimeoutAndCommitsWhatWasReported() throws Exception {
    messenger = createMessenger(200);
    messenger.sendForIndexing(kafkaDoc(a0, 9));
    offsets.add(Map.of(a0, new OffsetAndMetadata(5)));

    long start = System.nanoTime();
    messenger.onPartitionsRevoked(List.of(a0));
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

    verify(consumer, times(1)).commitSync(Map.of(a0, new OffsetAndMetadata(5)));
    assertTrue("took " + elapsedMs + " ms", elapsedMs >= 200 && elapsedMs < 5000);
  }

  @Test(timeout = 10000)
  public void testRevokeDoesNotWaitForPartitionsWithNothingInFlight() throws Exception {
    messenger.sendForIndexing(kafkaDoc(a0, 2));
    offsets.add(Map.of(a0, new OffsetAndMetadata(3)));
    // b0 has unreported work, but it is not being revoked
    messenger.sendForIndexing(kafkaDoc(b0, 7));

    messenger.onPartitionsRevoked(List.of(a0, a1));

    verify(consumer, times(1)).commitSync(Map.of(a0, new OffsetAndMetadata(3)));
  }

  @Test(timeout = 10000)
  public void testRevokeDuringCloseDoesNotWaitForTheStoppedIndexer() throws Exception {
    messenger.sendForIndexing(kafkaDoc(a0, 9));
    offsets.add(Map.of(a0, new OffsetAndMetadata(5)));
    // KafkaConsumer.close() revokes the consumer's partitions through the listener
    doAnswer(invocation -> {
      messenger.onPartitionsRevoked(List.of(a0));
      return null;
    }).when(consumer).close();

    long start = System.nanoTime();
    messenger.close();
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

    verify(consumer, times(1)).commitSync(Map.of(a0, new OffsetAndMetadata(5)));
    assertTrue("close should not wait out the 10 s drain timeout, took " + elapsedMs + " ms", elapsedMs < 5000);
  }

  @Test(timeout = 10000)
  public void testIndexerStoppingMidWaitEndsTheWait() throws Exception {
    messenger.sendForIndexing(kafkaDoc(a0, 9));
    offsets.add(Map.of(a0, new OffsetAndMetadata(5)));
    Thread stopper = new Thread(() -> {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      messenger.indexerStopping();
    });
    stopper.start();

    long start = System.nanoTime();
    messenger.onPartitionsRevoked(List.of(a0));
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

    verify(consumer, times(1)).commitSync(Map.of(a0, new OffsetAndMetadata(5)));
    assertTrue("should stop waiting soon after the indexer stops, took " + elapsedMs + " ms", elapsedMs < 2000);
  }

  @Test
  public void testRevokeCommitFailureDoesNotEscapeTheListener() throws Exception {
    offsets.add(Map.of(a0, new OffsetAndMetadata(5)));
    doThrow(new CommitFailedException()).when(consumer).commitSync(anyMap());

    messenger.onPartitionsRevoked(List.of(a0));

    // the revoked partition's state is forgotten; nothing is retried for it
    when(consumer.assignment()).thenReturn(Set.of(a1, b0));
    messenger.commitPendingDocOffsets();
    verify(consumer, times(1)).commitSync(anyMap());
  }

  @Test
  public void testLostPartitionsAreNotCommitted() throws Exception {
    messenger.sendForIndexing(kafkaDoc(a0, 4));
    offsets.add(Map.of(a0, new OffsetAndMetadata(5)));
    messenger.commitPendingDocOffsets();
    offsets.add(Map.of(a0, new OffsetAndMetadata(9)));

    messenger.onPartitionsLost(List.of(a0));

    verify(consumer, times(1)).commitSync(anyMap());
  }

  @Test
  public void testReportsForPartitionsNoLongerOwnedAreDropped() throws Exception {
    when(consumer.assignment()).thenReturn(Set.of(a0));
    offsets.add(Map.of(a0, new OffsetAndMetadata(3), b0, new OffsetAndMetadata(7)));

    messenger.commitPendingDocOffsets();

    verify(consumer, times(1)).commitSync(Map.of(a0, new OffsetAndMetadata(3)));
  }

  @Test
  public void testFailedCommitIsRetriedOnTheNextCall() throws Exception {
    offsets.add(Map.of(a0, new OffsetAndMetadata(3)));
    doThrow(new CommitFailedException()).doNothing().when(consumer).commitSync(anyMap());

    assertThrows(CommitFailedException.class, () -> messenger.commitPendingDocOffsets());
    messenger.commitPendingDocOffsets();
    messenger.commitPendingDocOffsets();

    verify(consumer, times(2)).commitSync(Map.of(a0, new OffsetAndMetadata(3)));
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
}
