package com.kmwllc.lucille.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.KafkaDocument;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests how HybridWorkerMessenger commits source offsets on behalf of its paired Indexer, and how it handles partitions
 * it gives up, using a MockConsumer, which tracks committed offsets and drives rebalances.
 */
public class HybridWorkerMessengerTest {

  private static final String TOPIC = "pipeline1_source";
  private static final TopicPartition P0 = new TopicPartition(TOPIC, 0);
  private static final TopicPartition P1 = new TopicPartition(TOPIC, 1);

  private final AtomicInteger commits = new AtomicInteger();
  // MockConsumer reports 0 as the committed offset of a partition it is not assigned, so commits are recorded here
  private final Map<TopicPartition, Long> committedOffsets = new ConcurrentHashMap<>();
  private MockConsumer<String, KafkaDocument> consumer;
  private LinkedBlockingQueue<Document> pipelineDest;
  private HybridWorkerMessenger messenger;

  @Before
  public void setUp() {
    consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
      @Override
      public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        commits.incrementAndGet();
        offsets.forEach((partition, offset) -> committedOffsets.put(partition, offset.offset()));
        super.commitSync(offsets);
      }
    };
    // the real document consumer reads one record per poll
    consumer.setMaxPollRecords(1);
    pipelineDest = new LinkedBlockingQueue<>();
    messenger = new HybridWorkerMessenger(config(300000), "pipeline1", pipelineDest, consumer);
    assign(P0, P1);
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
    messenger = new HybridWorkerMessenger(config(1000), "pipeline1", pipelineDest, consumer);
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
}
