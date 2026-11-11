package com.kmwllc.lucille.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests how HybridWorkerMessenger commits source offsets on behalf of its paired Indexer, and how it handles partitions
 * it gives up, using a MockConsumer, which tracks committed offsets and drives rebalances.
 */
public class HybridWorkerMessengerTest {

  // a pipeline name no other test uses, so the metrics in the shared registry are this test's alone
  private static final String PIPELINE = "hybridWorkerMessengerTest";
  private static final String TOPIC = PIPELINE + "_source";
  private static final TopicPartition P0 = new TopicPartition(TOPIC, 0);
  private static final TopicPartition P1 = new TopicPartition(TOPIC, 1);

  private final AtomicInteger commits = new AtomicInteger();
  // when set, commitSync throws it
  private volatile RuntimeException commitException;
  // MockConsumer reports 0 as the committed offset of a partition it is not assigned, so commits are recorded here
  private final Map<TopicPartition, Long> committedOffsets = new ConcurrentHashMap<>();
  private MockConsumer<String, KafkaDocument> consumer;
  private LinkedBlockingQueue<Document> pipelineDest;
  private HybridWorkerMessenger messenger;
  private final MetricRegistry metrics = SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG);

  @Before
  public void setUp() {
    removeMetrics();
    consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
      @Override
      public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (commitException != null) {
          throw commitException;
        }
        commits.incrementAndGet();
        offsets.forEach((partition, offset) -> committedOffsets.put(partition, offset.offset()));
        super.commitSync(offsets);
      }
    };
    // the real document consumer reads one record per poll
    consumer.setMaxPollRecords(1);
    pipelineDest = new LinkedBlockingQueue<>();
    messenger = new HybridWorkerMessenger(config(300000), PIPELINE, pipelineDest, consumer);
    assign(P0, P1);
  }

  @After
  public void tearDown() {
    removeMetrics();
  }

  private void removeMetrics() {
    metrics.removeMatching(MetricFilter.startsWith(PIPELINE + "."));
  }

  private static Config config(int maxPollIntervalMs) {
    return ConfigFactory.parseString("kafka { bootstrapServers: \"localhost:9092\", consumerGroupId: \"group\", "
        + "maxPollIntervalMs: " + maxPollIntervalMs + ", maxRequestSize: 1048576, events: false }");
  }

  private void assign(TopicPartition... partitions) {
    consumer.rebalance(Set.of(partitions));
    for (TopicPartition partition : partitions) {
      consumer.updateBeginningOffsets(Map.of(partition, 0L));
    }
  }

  // Adds a record to the partition, for the next poll.
  private void addRecord(TopicPartition partition, long offset) throws Exception {
    String id = "doc-" + partition.partition() + "-" + offset;
    KafkaDocument value = new KafkaDocument((ObjectNode) new ObjectMapper().readTree(Document.create(id, "run1").toString()));
    consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(), offset, id, value));
  }

  // Adds a record, polls it as the Worker would and hands it to the Indexer's queue.
  private KafkaDocument poll(TopicPartition partition, long offset) throws Exception {
    addRecord(partition, offset);
    KafkaDocument doc = messenger.pollDocToProcess();
    messenger.sendForIndexing(doc);
    return doc;
  }

  private Long committed(TopicPartition partition) {
    return committedOffsets.get(partition);
  }

  @Test
  public void testNothingIsCommittedUntilABatchCompletes() throws Exception {
    poll(P0, 0);
    messenger.commitPendingDocOffsets();
    assertNull(committed(P0));
  }

  @Test
  public void testCommitDoesNotSkipAnUncompletedEarlierRecord() throws Exception {
    // with indexOverrideField, records can complete out of offset order
    KafkaDocument offset0 = poll(P0, 0);
    KafkaDocument offset1 = poll(P0, 1);
    KafkaDocument offset2 = poll(P0, 2);

    messenger.batchCompleted(List.of(offset0, offset2));
    messenger.commitPendingDocOffsets();
    assertEquals("must not commit past the uncompleted offset 1", Long.valueOf(1L), committed(P0));

    messenger.batchCompleted(List.of(offset1));
    messenger.commitPendingDocOffsets();
    assertEquals(Long.valueOf(3L), committed(P0));
  }

  @Test
  public void testDroppedDocumentDoesNotStallCommits() throws Exception {
    // offset 0 is dropped by the pipeline: polled, but never sent for indexing
    String id = "doc-0-0";
    KafkaDocument value = new KafkaDocument((ObjectNode) new ObjectMapper().readTree(Document.create(id, "run1").toString()));
    consumer.addRecord(new ConsumerRecord<>(P0.topic(), P0.partition(), 0L, id, value));
    messenger.pollDocToProcess();

    KafkaDocument offset1 = poll(P0, 1);
    KafkaDocument offset2 = poll(P0, 2);
    messenger.batchCompleted(List.of(offset1, offset2));
    messenger.commitPendingDocOffsets();

    assertEquals(Long.valueOf(3L), committed(P0));
  }

  @Test
  public void testRecordThatSendsNothingIsFinishedByTheCommit() throws Exception {
    // dropped or failed in the pipeline, or sent to the dead letter queue: the worker commits after it, without polling again
    addRecord(P0, 0);
    messenger.pollDocToProcess();
    messenger.commitPendingDocOffsets();
    assertEquals(Long.valueOf(1L), committed(P0));
  }

  @Test
  public void testDroppedParentWaitsForItsChildren() throws Exception {
    // a parent dropped after a stage created two children from it, as EmitNestedChildren does with dropParent
    addRecord(P0, 0);
    messenger.pollDocToProcess();
    Document child1 = Document.create("child1");
    Document child2 = Document.create("child2");
    messenger.sendForIndexing(child1);
    messenger.sendForIndexing(child2);
    messenger.commitPendingDocOffsets();
    assertNull("the children are not indexed yet", committed(P0));

    messenger.batchCompleted(List.of(child1));
    messenger.commitPendingDocOffsets();
    assertNull("child2 is not indexed yet", committed(P0));

    messenger.batchCompleted(List.of(child2));
    messenger.commitPendingDocOffsets();
    assertEquals(Long.valueOf(1L), committed(P0));
  }

  @Test
  public void testIndexedParentWaitsForItsChildren() throws Exception {
    // children are sent before their parent, but a concurrent Indexer can complete the parent's batch first
    addRecord(P0, 0);
    KafkaDocument parent = messenger.pollDocToProcess();
    Document child = Document.create("child");
    messenger.sendForIndexing(child);
    messenger.sendForIndexing(parent);

    messenger.batchCompleted(List.of(parent));
    messenger.commitPendingDocOffsets();
    assertNull("the child is not indexed yet", committed(P0));

    messenger.batchCompleted(List.of(child));
    messenger.commitPendingDocOffsets();
    assertEquals(Long.valueOf(1L), committed(P0));
  }

  @Test
  public void testChildNotSentRewindsItsRecord() throws Exception {
    addRecord(P0, 0);
    messenger.pollDocToProcess();
    Document child = Document.create("child");
    messenger.sendForIndexing(child);
    pipelineDest.remove(child);   // taken by the Indexer
    messenger.commitPendingDocOffsets();

    // the Indexer could not confirm ownership and skipped the child's batch, though P0 is still assigned
    messenger.batchNotSent(List.of(child));
    assertNull(messenger.pollDocToProcess());

    assertNull(committed(P0));
    assertEquals("the record is polled again, so its child is created and indexed again", 0L, consumer.position(P0));
  }

  @Test
  public void testChildTakenByTheIndexerIsFencedWhenItsPartitionIsRevoked() throws Exception {
    addRecord(P0, 0);
    messenger.pollDocToProcess();
    Document child = Document.create("child");
    messenger.sendForIndexing(child);
    pipelineDest.remove(child);   // taken by the Indexer, so not purged
    messenger.commitPendingDocOffsets();
    assertTrue(messenger.ownsInput(child));

    consumer.rebalance(Set.of(P1));

    assertFalse("the child's record is from a revoked partition", messenger.ownsInput(child));
  }

  @Test
  public void testLateReportCannotFinishTheRecordPolledAgain() throws Exception {
    addRecord(P0, 0);
    messenger.pollDocToProcess();
    Document oldChild = Document.create("child");
    messenger.sendForIndexing(oldChild);
    pipelineDest.remove(oldChild);   // taken by the Indexer
    messenger.commitPendingDocOffsets();

    // P0 is revoked and comes back, so offset 0 is polled and processed again
    consumer.rebalance(Set.of(P1));
    assign(P0, P1);
    addRecord(P0, 0);
    messenger.pollDocToProcess();
    Document newChild = Document.create("child");
    messenger.sendForIndexing(newChild);
    messenger.commitPendingDocOffsets();

    // the Indexer reports the child from before the revocation
    messenger.batchCompleted(List.of(oldChild));
    messenger.commitPendingDocOffsets();
    assertNull("offset 0 is still waiting for its new child", committed(P0));

    messenger.batchCompleted(List.of(newChild));
    messenger.commitPendingDocOffsets();
    assertEquals(Long.valueOf(1L), committed(P0));
  }

  @Test
  public void testRecordStillBeingProcessedIsNotCommittedOnClose() throws Exception {
    // the worker stopped while processing the record, after a messenger failure: it must be redelivered
    addRecord(P0, 0);
    messenger.pollDocToProcess();
    messenger.close();
    assertNull(committed(P0));
  }

  @Test
  public void testBatchesCompletedTogetherAreCommittedOnce() throws Exception {
    KafkaDocument offset0 = poll(P0, 0);
    KafkaDocument offset1 = poll(P0, 1);
    KafkaDocument p1Offset0 = poll(P1, 0);
    messenger.batchCompleted(List.of(offset0));
    messenger.batchCompleted(List.of(offset1, p1Offset0));

    int before = commits.get();
    messenger.commitPendingDocOffsets();
    assertEquals(1, commits.get() - before);
    assertEquals(Long.valueOf(2L), committed(P0));
    assertEquals(Long.valueOf(1L), committed(P1));
  }

  @Test
  public void testRevocationCommitsCompletedBatchesAndRemovesQueuedDocuments() throws Exception {
    KafkaDocument p0Offset0 = poll(P0, 0);
    pipelineDest.remove(p0Offset0);   // taken by the Indexer
    poll(P0, 1);                      // still waiting
    KafkaDocument p1Offset0 = poll(P1, 0);
    messenger.batchCompleted(List.of(p0Offset0));

    // P0 is revoked before the worker commits
    consumer.rebalance(Set.of(P1));

    assertEquals("the completed batch is committed before the handoff", Long.valueOf(1L), committed(P0));
    assertEquals("P0's waiting document is left for its next owner", List.of(p1Offset0), List.copyOf(pipelineDest));
  }

  @Test
  public void testLateCompletionCannotRewindTheNextOwner() throws Exception {
    KafkaDocument p0Offset0 = poll(P0, 0);
    KafkaDocument p0Offset1 = poll(P0, 1);

    // P0 moves to another worker, which commits further, then comes back
    consumer.rebalance(Set.of(P1));
    consumer.commitSync(Map.of(P0, new OffsetAndMetadata(10)));
    consumer.rebalance(Set.of(P0, P1));

    messenger.batchCompleted(List.of(p0Offset0, p0Offset1));
    messenger.commitPendingDocOffsets();
    assertEquals(Long.valueOf(10L), committed(P0));
  }

  @Test
  public void testOwnershipEndsWhenThePartitionIsRevoked() throws Exception {
    KafkaDocument p0Offset0 = poll(P0, 0);
    KafkaDocument p1Offset0 = poll(P1, 0);
    assertTrue(messenger.ownsInput(p0Offset0));
    // a child a stage created has no partition
    assertTrue(messenger.ownsInput(Document.create("child")));

    consumer.rebalance(Set.of(P1));

    assertFalse(messenger.ownsInput(p0Offset0));
    assertTrue(messenger.ownsInput(p1Offset0));
  }

  @Test
  public void testOwnershipEndsAfterThePollDeadline() throws Exception {
    messenger = new HybridWorkerMessenger(config(1000), PIPELINE, pipelineDest, consumer);
    assign(P0);
    KafkaDocument doc = poll(P0, 0);
    assertTrue(messenger.ownsInput(doc));

    Thread.sleep(1100);
    assertFalse(messenger.ownsInput(doc));
  }

  @Test
  public void testUnsentBatchOfAStillOwnedPartitionIsPolledAgain() throws Exception {
    KafkaDocument offset0 = poll(P0, 0);
    pipelineDest.remove(offset0);   // taken by the Indexer
    poll(P0, 1);                    // still waiting

    // the Indexer could not confirm ownership in time and skipped offset 0's batch, though P0 is still assigned
    messenger.batchNotSent(List.of(offset0));
    assertNull(messenger.pollDocToProcess());

    assertTrue("the partition's waiting documents will be polled again", pipelineDest.isEmpty());
    assertEquals(0L, consumer.position(P0));
  }

  @Test
  public void testPollIsTimed() throws Exception {
    messenger.pollDocToProcess();
    addRecord(P0, 0);
    messenger.pollDocToProcess();
    assertEquals(2, metrics.timer(PIPELINE + KafkaConsumerMetrics.WORKER_POLL_TIME_SUFFIX).getCount());
  }

  @Test
  public void testPollIsTimedWhenItThrows() throws Exception {
    consumer.wakeup();
    assertThrows(WakeupException.class, () -> messenger.pollDocToProcess());
    assertEquals(1, metrics.timer(PIPELINE + KafkaConsumerMetrics.WORKER_POLL_TIME_SUFFIX).getCount());
  }

  @Test
  public void testCommitIsTimedOnlyWhenThereIsSomethingToCommit() throws Exception {
    messenger.commitPendingDocOffsets();
    assertEquals(0, metrics.timer(PIPELINE + KafkaConsumerMetrics.WORKER_COMMIT_TIME_SUFFIX).getCount());

    KafkaDocument offset0 = poll(P0, 0);
    KafkaDocument offset1 = poll(P0, 1);
    messenger.batchCompleted(List.of(offset0));
    messenger.batchCompleted(List.of(offset1));
    messenger.commitPendingDocOffsets();
    assertEquals(1, metrics.timer(PIPELINE + KafkaConsumerMetrics.WORKER_COMMIT_TIME_SUFFIX).getCount());
    assertEquals(0, metrics.meter(PIPELINE + KafkaConsumerMetrics.WORKER_COMMIT_FAILURES_SUFFIX).getCount());
  }

  @Test
  public void testCommitFailureIsCountedAndRethrown() throws Exception {
    CommitFailedException failure = new CommitFailedException();
    commitException = failure;
    KafkaDocument offset0 = poll(P0, 0);
    messenger.batchCompleted(List.of(offset0));

    CommitFailedException thrown = assertThrows(CommitFailedException.class, () -> messenger.commitPendingDocOffsets());
    assertSame(failure, thrown);
    assertEquals(1, metrics.meter(PIPELINE + KafkaConsumerMetrics.WORKER_COMMIT_FAILURES_SUFFIX).getCount());
    assertEquals(1, metrics.timer(PIPELINE + KafkaConsumerMetrics.WORKER_COMMIT_TIME_SUFFIX).getCount());
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
    pipelineDest = new LinkedBlockingQueue<>(1);
    messenger = new HybridWorkerMessenger(config(300000), PIPELINE, pipelineDest, consumer);
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

  @Test
  public void testSendToFullQueueStillTracksItsRecord() throws Exception {
    // a document that waited for space must still hold back its record's commit until the Indexer reports it
    pipelineDest = new LinkedBlockingQueue<>(1);
    messenger = new HybridWorkerMessenger(config(300000), PIPELINE, pipelineDest, consumer);
    assign(P0);
    addRecord(P0, 0);
    messenger.pollDocToProcess();
    Document first = Document.create("child1");
    Document second = Document.create("child2");
    messenger.sendForIndexing(first);

    Thread sender = new Thread(() -> {
      try {
        messenger.sendForIndexing(second);
      } catch (MessengerException e) {
        throw new RuntimeException(e);
      }
    });
    sender.start();
    assertSame(first, pipelineDest.take());
    sender.join(10_000);
    assertSame(second, pipelineDest.take());

    messenger.batchCompleted(List.of(first));
    messenger.commitPendingDocOffsets();
    assertNull("child2 is not indexed yet", committed(P0));

    messenger.batchCompleted(List.of(second));
    messenger.commitPendingDocOffsets();
    assertEquals(Long.valueOf(1L), committed(P0));
  }

  @Test
  public void testCommitFailureOnRevocationIsCounted() throws Exception {
    KafkaDocument offset0 = poll(P0, 0);
    pipelineDest.remove(offset0);   // taken by the Indexer
    poll(P1, 0);                    // the worker is done with offset 0 once it polls again
    messenger.batchCompleted(List.of(offset0));
    commitException = new CommitFailedException();

    // P0 is revoked before the worker commits; the commit made during the revocation fails, and is only logged
    consumer.rebalance(Set.of(P1));

    assertNull(committed(P0));
    assertEquals(1, metrics.meter(PIPELINE + KafkaConsumerMetrics.WORKER_COMMIT_FAILURES_SUFFIX).getCount());
    assertEquals(1, metrics.timer(PIPELINE + KafkaConsumerMetrics.WORKER_COMMIT_TIME_SUFFIX).getCount());
  }
}
