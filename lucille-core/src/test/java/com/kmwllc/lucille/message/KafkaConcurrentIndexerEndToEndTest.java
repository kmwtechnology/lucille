package com.kmwllc.lucille.message;

import com.kmwllc.lucille.util.EmbeddedKafkaUtils;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerContext;
import com.kmwllc.lucille.core.IndexerFailure;
import com.kmwllc.lucille.core.PendingSend;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

/**
 * End-to-end test of a concurrent ({@code indexer.maxConcurrentBatches > 1}) indexer driven through a real
 * {@link KafkaIndexerMessenger} against an embedded broker. Where {@link KafkaIndexerMessengerCommitTest} drives the
 * messenger's commit bookkeeping directly with a MockConsumer, this runs an actual {@link Indexer#run()} loop over
 * documents on multiple partitions and asserts that: offsets are committed only after batches complete, the sends
 * really do overlap (peak concurrency greater than one), and every document's FINISH event is emitted.
 *
 * <p> Adapted from the testConcurrentIndexerEndToEnd case in kmwtechnology/lucille PR #575 (which was removed there when
 * concurrent batches were disabled for the standalone Kafka indexer; this PR keeps the Kafka path and so keeps the test).
 */
public class KafkaConcurrentIndexerEndToEndTest {

  private static final long TIMEOUT_MS = 30000;
  private static final AtomicInteger counter = new AtomicInteger();

  private static EmbeddedKafkaBroker embeddedKafka;
  private static Admin admin;

  private KafkaProducer<String, Document> producer;

  @BeforeClass
  public static void startKafka() {
    // a warmed-up broker, so the one-time creation of __consumer_offsets does not land inside a test's time budget
    embeddedKafka = EmbeddedKafkaUtils.startBroker();
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
    admin = Admin.create(props);
  }

  @AfterClass
  public static void stopKafka() {
    if (admin != null) {
      admin.close();
    }
    if (embeddedKafka != null) {
      embeddedKafka.destroy();
    }
  }

  @After
  public void tearDown() throws Exception {
    if (producer != null) {
      producer.close();
    }
  }

  @Test
  public void testConcurrentIndexerCommitsEverythingAndOverlapsSends() throws Exception {
    String pipeline = newPipeline(2);
    String runId = "run-" + pipeline;
    Config config = config(pipeline, 3, Map.of(
        "kafka.events", true, "indexer.batchSize", 2, "indexer.batchTimeoutMs", 20, "kafka.pollIntervalMs", 25));
    int perPartition = 15;

    Set<String> ids = new HashSet<>();
    ids.addAll(produce(config, pipeline, 0, perPartition, runId));
    ids.addAll(produce(config, pipeline, 1, perPartition, runId));

    String topic = KafkaUtils.getDestTopicName(pipeline);
    Map<TopicPartition, Long> expectedCommits = Map.of(
        new TopicPartition(topic, 0), (long) perPartition,
        new TopicPartition(topic, 1), (long) perPartition);

    KafkaIndexerMessenger messenger = new KafkaIndexerMessenger(config, pipeline);
    SlowIndexer indexer = new SlowIndexer(config.getConfig("indexer"), messenger);
    Thread thread = new Thread(indexer);
    thread.start();
    try {
      // All documents indexed and their offsets committed (the frontier reaches the end of each partition).
      waitFor(() -> expectedCommits.equals(committed(config)));
    } finally {
      indexer.terminate();
      thread.join(TIMEOUT_MS);
    }

    assertEquals(expectedCommits, committed(config));
    // The point of the feature: with maxConcurrentBatches > 1 and slow sends, batches really did overlap.
    assertTrue("expected overlapping sends, peak was " + indexer.peakConcurrent.get(),
        indexer.peakConcurrent.get() > 1);

    // Every document produced a FINISH event, so nothing was dropped end to end.
    Set<String> finished = new HashSet<>();
    try (KafkaConsumer<String, String> events = eventConsumer(config)) {
      events.subscribe(List.of(KafkaUtils.getEventTopicName(config, pipeline, runId)));
      long deadline = System.currentTimeMillis() + TIMEOUT_MS;
      while (!finished.containsAll(ids) && System.currentTimeMillis() < deadline) {
        for (ConsumerRecord<String, String> record : events.poll(Duration.ofMillis(200))) {
          Event event = Event.fromJsonString(record.value());
          if (event.getType() == Event.Type.FINISH) {
            finished.add(event.getDocumentId());
          }
        }
      }
    }
    assertEquals(ids, finished);
  }

  /**
   * A rebalance that starts while the indexer thread is blocked in dispatch() must not re-deliver or reorder anything.
   * Every document writes the same destination id, so each one-document batch waits for the one before it. doc0's send
   * is held, so dispatching doc1 blocks, and another member joins the group during that wait. The indexer does not
   * poll while it is blocked, so the rebalance waits for it. Once doc0's send is released, doc1 is submitted, the run
   * loop polls, and only then does the revocation hook flush doc2 and drain, so the hook never runs in the middle of a
   * dispatch.
   */
  @Test
  public void testRebalanceDuringBlockedDispatchDoesNotResendOrReorder() throws Exception {
    String pipeline = newPipeline(1);
    // One document per batch, never flushed by time.
    Config config = config(pipeline, 2, Map.of("indexer.batchSize", 1, "indexer.batchTimeoutMs", 600000,
        "indexer.idOverrideField", "destId", "kafka.pollIntervalMs", 100));
    String topic = KafkaUtils.getDestTopicName(pipeline);
    TopicPartition partition = new TopicPartition(topic, 0);
    int total = 5;
    if (producer == null) {
      producer = KafkaUtils.createDocumentProducer(config);
    }
    List<String> ids = new ArrayList<>();
    for (int i = 0; i < total; i++) {
      String id = "doc" + i;
      Document doc = Document.create(id, "run");
      doc.setField("destId", "x");
      producer.send(new ProducerRecord<>(topic, 0, id, doc)).get();
      ids.add(id);
    }

    AtomicInteger polled = new AtomicInteger();
    AtomicInteger pollsSinceRejoin = new AtomicInteger();
    KafkaIndexerMessenger messenger = new KafkaIndexerMessenger(config, pipeline) {
      @Override
      public Document pollDocToIndex() throws MessengerException {
        Document doc = super.pollDocToIndex();
        if (doc != null) {
          polled.incrementAndGet();
        }
        // Runs on the indexer thread, which owns the consumer, so reading the assignment here is safe.
        if (assignment().contains(partition)) {
          pollsSinceRejoin.incrementAndGet();
        } else {
          pollsSinceRejoin.set(0);
        }
        return doc;
      }
    };
    GatedIndexer indexer = new GatedIndexer(config.getConfig("indexer"), messenger, "doc0");
    Thread thread = new Thread(indexer);
    thread.start();
    try {
      // Polling doc1 dispatches doc0, whose send is held. Polling doc2 dispatches doc1, which blocks behind doc0.
      waitFor(() -> indexer.gatedSendStarted && polled.get() == 3);

      try (KafkaConsumer<String, String> joiner = groupMember(config)) {
        AtomicInteger assignments = new AtomicInteger();
        joiner.subscribe(List.of(topic), new ConsumerRebalanceListener() {
          @Override
          public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
          }

          @Override
          public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            joiner.pause(partitions);
            assignments.incrementAndGet();
          }
        });
        boolean released = false;
        long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (assignments.get() == 0 && System.currentTimeMillis() < deadline) {
          joiner.poll(Duration.ofMillis(100));
          if (!released && groupState(config) == GroupState.PREPARING_REBALANCE) {
            // The rebalance is waiting on the indexer, which is still blocked in dispatch and has not polled since.
            assertEquals(3, polled.get());
            indexer.release();
            released = true;
          }
        }
        assertTrue("the rebalance never started while the indexer was blocked", released);
        assertTrue("joiner never completed a rebalance", assignments.get() > 0);
      }

      // The joiner has left; let the indexer take the partition back and poll the rest.
      pollsSinceRejoin.set(0);
      waitFor(() -> pollsSinceRejoin.get() >= 20 && polled.get() >= total);
      Thread.sleep(1000);
    } finally {
      indexer.release();
      indexer.terminate();
      thread.join(TIMEOUT_MS);
    }

    // Each document was polled and sent exactly once, in order, and every offset was committed.
    assertEquals("a re-delivery would poll some documents twice", total, polled.get());
    assertEquals(ids, indexer.sent);
    assertEquals(Map.of(partition, (long) total), committed(config));
  }

  /**
   * A consumer-group rebalance while documents sit in the indexer's open batch must not make the indexer send them
   * twice. Lucille's documented config puts the worker, indexer, and publisher in one consumer group, so every component
   * that joins revokes the indexer's partitions. With commit-on-completion, documents polled but not yet completed are
   * re-read from the committed offset after the rebalance and land in the batch a second time, so each is indexed (and
   * emits FINISH) twice. Committing at poll, as before, never re-delivered on a rebalance.
   */
  @Test
  public void testRebalanceDoesNotResendBatchedDocuments() throws Exception {
    String pipeline = newPipeline(1);
    // A batch that neither fills nor expires, so every polled document is still in the open batch at the rebalance.
    Config config = config(pipeline, 1, Map.of("indexer.batchSize", 1000, "indexer.batchTimeoutMs", 600000,
        "kafka.pollIntervalMs", 100));
    int total = 5;
    List<String> ids = produce(config, pipeline, 0, total, "run");
    TopicPartition partition = new TopicPartition(KafkaUtils.getDestTopicName(pipeline), 0);

    AtomicInteger polled = new AtomicInteger();
    AtomicInteger pollsSinceRejoin = new AtomicInteger();
    KafkaIndexerMessenger messenger = new KafkaIndexerMessenger(config, pipeline) {
      @Override
      public Document pollDocToIndex() throws MessengerException {
        Document doc = super.pollDocToIndex();
        if (doc != null) {
          polled.incrementAndGet();
        }
        // Runs on the indexer thread, which owns the consumer, so reading the assignment here is safe.
        if (assignment().contains(partition)) {
          pollsSinceRejoin.incrementAndGet();
        } else {
          pollsSinceRejoin.set(0);
        }
        return doc;
      }
    };
    RecordingIndexer indexer = new RecordingIndexer(config.getConfig("indexer"), messenger);
    Thread thread = new Thread(indexer);
    thread.start();
    try {
      waitFor(() -> polled.get() == total);

      // Another member joins the indexer's consumer group, forcing a rebalance, then leaves (a second rebalance). It
      // pauses whatever it is assigned and never commits, so it consumes nothing.
      try (KafkaConsumer<String, String> joiner = groupMember(config)) {
        AtomicInteger assignments = new AtomicInteger();
        joiner.subscribe(List.of(partition.topic()), new ConsumerRebalanceListener() {
          @Override
          public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
          }

          @Override
          public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            joiner.pause(partitions);
            assignments.incrementAndGet();
          }
        });
        long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (assignments.get() == 0 && System.currentTimeMillis() < deadline) {
          joiner.poll(Duration.ofMillis(100));
        }
        assertTrue("joiner never completed a rebalance", assignments.get() > 0);
      }

      // Let the indexer take the partition back and settle; a re-delivery of these few records takes milliseconds.
      pollsSinceRejoin.set(0);
      waitFor(() -> pollsSinceRejoin.get() >= 20);
      Thread.sleep(2000);
    } finally {
      indexer.terminate();
      thread.join(TIMEOUT_MS);
    }

    // The final flush on terminate sent the open batch: each document exactly once.
    assertEquals(ids, indexer.sent);
  }

  /**
   * Runs several hundred adds and deletes over a handful of destination ids through a real Kafka consumer, a concurrent
   * indexer and a simulated destination, and checks the destination ends up exactly as the operations, in the order
   * the indexer received them, imply. Each id's operations must be applied in received order (so a delete can never
   * overtake the add before it, nor an earlier add land after a later one), every offset must be committed, and every
   * document must emit FINISH. One partition keeps "the order the indexer received" well defined.
   */
  @Test
  public void testRandomAddDeleteSequenceAcrossManyIds() throws Exception {
    String pipeline = newPipeline(1);
    String runId = "run-" + pipeline;
    Config config = config(pipeline, 4, Map.of("kafka.events", true, "indexer.batchSize", 4,
        "indexer.batchTimeoutMs", 20, "indexer.idOverrideField", "destId", "kafka.pollIntervalMs", 25));
    int total = 600;
    String[] destIds = new String[60];
    for (int i = 0; i < destIds.length; i++) {
      destIds[i] = "id" + i;
    }
    Random random = new Random(11);
    String topic = KafkaUtils.getDestTopicName(pipeline);
    if (producer == null) {
      producer = KafkaUtils.createDocumentProducer(config);
    }
    Set<String> ids = new HashSet<>();
    for (int i = 0; i < total; i++) {
      String id = "op" + i;
      Document doc = Document.create(id, runId);
      doc.setField("destId", destIds[random.nextInt(destIds.length)]);
      boolean isAdd = random.nextInt(3) != 0;
      doc.setField("op", isAdd ? "add" : "delete");
      doc.setField("version", String.valueOf(i));
      producer.send(new ProducerRecord<>(topic, 0, id, doc));
      ids.add(id);
    }
    producer.flush();

    // The order the indexer receives the documents, which is the order the destination must reflect.
    List<Document> received = Collections.synchronizedList(new ArrayList<>());
    KafkaIndexerMessenger messenger = new KafkaIndexerMessenger(config, pipeline) {
      @Override
      public Document pollDocToIndex() throws MessengerException {
        Document doc = super.pollDocToIndex();
        if (doc != null) {
          received.add(doc);
        }
        return doc;
      }
    };
    DestinationIndexer indexer = new DestinationIndexer(config.getConfig("indexer"), messenger);
    Map<TopicPartition, Long> expectedCommits = Map.of(new TopicPartition(topic, 0), (long) total);
    Thread thread = new Thread(indexer);
    thread.start();
    try {
      waitFor(() -> received.size() == total);
      waitFor(() -> expectedCommits.equals(committed(config)));
    } finally {
      indexer.terminate();
      thread.join(TIMEOUT_MS);
    }

    // The model: replay the operations in received order.
    Map<String, String> expectedContent = new HashMap<>();
    Map<String, List<String>> expectedOrder = new HashMap<>();
    for (Document doc : received) {
      String destId = doc.getString("destId");
      if (doc.getString("op").equals("add")) {
        expectedContent.put(destId, doc.getString("version"));
      } else {
        expectedContent.remove(destId);
      }
      expectedOrder.computeIfAbsent(destId, k -> new ArrayList<>()).add(doc.getId());
    }
    assertEquals(expectedOrder, indexer.appliedOrder());
    assertEquals(expectedContent, indexer.content());
    assertTrue("expected overlapping sends, peak was " + indexer.peakConcurrent.get(), indexer.peakConcurrent.get() > 1);

    Set<String> finished = new HashSet<>();
    try (KafkaConsumer<String, String> events = eventConsumer(config)) {
      TopicPartition eventPartition = new TopicPartition(KafkaUtils.getEventTopicName(config, pipeline, runId), 0);
      events.assign(List.of(eventPartition));
      events.seekToBeginning(List.of(eventPartition));
      long deadline = System.currentTimeMillis() + TIMEOUT_MS;
      while (!finished.containsAll(ids) && System.currentTimeMillis() < deadline) {
        for (ConsumerRecord<String, String> record : events.poll(Duration.ofMillis(50))) {
          Event event = Event.fromJsonString(record.value());
          if (event.getType() == Event.Type.FINISH) {
            finished.add(event.getDocumentId());
          }
        }
      }
    }
    assertEquals(ids, finished);
  }

  @Test
  public void testRejectedDocumentReachesFailTopic() throws Exception {
    String pipeline = newPipeline(1);
    String failTopic = KafkaUtils.getFailTopicName(pipeline);
    embeddedKafka.addTopics(new NewTopic(failTopic, 1, (short) 1));
    Config config = config(pipeline, 1, Map.of("indexer.batchTimeoutMs", 100, "indexer.sendFailedDocsToTopic", true));
    String id = produce(config, pipeline, 0, 1, "run").get(0);

    KafkaIndexerMessenger messenger = new KafkaIndexerMessenger(config, pipeline);
    RecordingIndexer indexer = new RecordingIndexer(config.getConfig("indexer"), messenger) {
      @Override
      protected List<IndexerFailure> sendToIndex(PendingSend send) {
        List<Document> documents = send.documents();
        return documents.stream().map(doc -> new IndexerFailure(doc, new IndexerException("rejected"))).toList();
      }
    };
    Thread thread = new Thread(indexer);
    thread.start();
    try {
      Document failed = readFailTopic(config, failTopic);
      assertEquals(id, failed.getId());
      assertEquals("indexer", failed.getString(Document.FAILURE_COMPONENT_FIELD));
      assertTrue(failed.getString(Document.FAILURE_REASON_FIELD).contains("rejected"));
      assertTrue(failed.has(Document.FAILURE_TIME_FIELD));
      // the fail-topic write came first, so the offset can now be committed
      waitFor(() -> Map.of(new TopicPartition(KafkaUtils.getDestTopicName(pipeline), 0), 1L).equals(committed(config)));
    } finally {
      indexer.terminate();
      thread.join(TIMEOUT_MS);
    }
  }

  @Test
  public void testWorkerJoiningDoesNotJoinTheIndexersGroup() throws Exception {
    String pipeline = newPipeline(1);
    embeddedKafka.addTopics(new NewTopic(KafkaUtils.getSourceTopicName(pipeline, ConfigFactory.empty()), 1, (short) 1));
    Config config = config(pipeline, 1, Map.of("indexer.batchTimeoutMs", 100));
    String id = produce(config, pipeline, 0, 1, "run").get(0);

    KafkaIndexerMessenger messenger = new KafkaIndexerMessenger(config, pipeline);
    RecordingIndexer indexer = new RecordingIndexer(config.getConfig("indexer"), messenger);
    Thread thread = new Thread(indexer);
    thread.start();
    String workersGroup = config.getString("kafka.consumerGroupId");
    try (KafkaConsumer<String, String> worker = groupMember(config, workersGroup)) {
      waitFor(() -> indexer.sent.contains(id));

      worker.subscribe(List.of(KafkaUtils.getSourceTopicName(pipeline, ConfigFactory.empty())));
      waitFor(() -> {
        worker.poll(Duration.ofMillis(100));
        return !worker.assignment().isEmpty();
      });

      Map<String, ConsumerGroupDescription> groups =
          admin.describeConsumerGroups(List.of(workersGroup, indexerGroup(config))).all().get();
      assertEquals(1, groups.get(workersGroup).members().size());
      assertEquals(1, groups.get(indexerGroup(config)).members().size());
      assertEquals(GroupState.STABLE, groups.get(indexerGroup(config)).groupState());
    } finally {
      indexer.terminate();
      thread.join(TIMEOUT_MS);
    }
  }

  @Test
  public void testHybridMessengersWriteToFailTopic() throws Exception {
    String pipeline = newPipeline(1);
    String failTopic = KafkaUtils.getFailTopicName(pipeline);
    embeddedKafka.addTopics(new NewTopic(failTopic, 1, (short) 1));

    Config workerConfig = config(pipeline, 1, Map.of("worker.maxRetries", 1));
    HybridWorkerMessenger workerMessenger =
        new HybridWorkerMessenger(workerConfig, pipeline, new LinkedBlockingQueue<>());
    try {
      workerMessenger.sendFailed(Document.create("fromWorker", "run"));
    } finally {
      workerMessenger.close();
    }
    assertEquals("fromWorker", readFailTopic(workerConfig, failTopic).getId());

    Config indexerConfig = config(pipeline, 1, Map.of("indexer.sendFailedDocsToTopic", true));
    // sendFailed and close don't use the worker messenger
    HybridIndexerMessenger indexerMessenger =
        new HybridIndexerMessenger(indexerConfig, new LinkedBlockingQueue<>(), null, null, pipeline);
    try {
      indexerMessenger.sendFailed(Document.create("fromIndexer", "run"));
    } finally {
      indexerMessenger.close();
    }
    List<String> ids = new ArrayList<>();
    try (KafkaConsumer<String, String> consumer = eventConsumer(indexerConfig)) {
      consumer.subscribe(List.of(failTopic));
      long deadline = System.currentTimeMillis() + TIMEOUT_MS;
      while (ids.size() < 2 && System.currentTimeMillis() < deadline) {
        for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
          ids.add(record.key());
        }
      }
    }
    assertEquals(List.of("fromWorker", "fromIndexer"), ids);
  }

  private static Document readFailTopic(Config config, String failTopic) throws Exception {
    try (KafkaConsumer<String, String> consumer = eventConsumer(config)) {
      consumer.subscribe(List.of(failTopic));
      long deadline = System.currentTimeMillis() + TIMEOUT_MS;
      while (System.currentTimeMillis() < deadline) {
        for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
          return Document.createFromJson(record.value());
        }
      }
    }
    throw new AssertionError("nothing was written to " + failTopic);
  }

  @Test
  public void testPublisherReadsEventsWithoutJoiningAGroup() throws Exception {
    String pipeline = newPipeline(1);
    Config config = config(pipeline, 1, Map.of("kafka.events", true));

    KafkaPublisherMessenger publisher = new KafkaPublisherMessenger(config);
    try (KafkaProducer<String, String> eventProducer = KafkaUtils.createEventProducer(config)) {
      publisher.initialize("run1", pipeline);
      String eventTopic = KafkaUtils.getEventTopicName(config, pipeline, "run1");
      sendEvent(eventProducer, eventTopic, "doc1");

      assertEquals("doc1", pollEvent(publisher).getDocumentId());

      String workersGroup = config.getString("kafka.consumerGroupId");
      ExecutionException noGroup =
          assertThrows(ExecutionException.class, () -> admin.describeConsumerGroups(List.of(workersGroup)).all().get());
      assertTrue(noGroup.getCause() instanceof GroupIdNotFoundException);
    } finally {
      publisher.close();
    }
  }

  @Test
  public void testPublisherSkipsEarlierRunsOnAFixedEventTopic() throws Exception {
    String pipeline = newPipeline(1);
    String eventTopic = pipeline + "_fixed_events";
    embeddedKafka.addTopics(new NewTopic(eventTopic, 1, (short) 1));
    Config config = config(pipeline, 1, Map.of("kafka.events", true, "kafka.eventTopic", eventTopic));

    KafkaPublisherMessenger publisher = new KafkaPublisherMessenger(config);
    try (KafkaProducer<String, String> eventProducer = KafkaUtils.createEventProducer(config)) {
      sendEvent(eventProducer, eventTopic, "fromEarlierRun");
      publisher.initialize("run1", pipeline);
      sendEvent(eventProducer, eventTopic, "fromThisRun");

      assertEquals("fromThisRun", pollEvent(publisher).getDocumentId());
    } finally {
      publisher.close();
    }
  }

  private static void sendEvent(KafkaProducer<String, String> producer, String topic, String id) throws Exception {
    Event event = new Event(Document.create(id, "run1"), "message", Event.Type.FINISH);
    producer.send(new ProducerRecord<>(topic, id, event.toString())).get();
  }

  private static Event pollEvent(KafkaPublisherMessenger publisher) throws Exception {
    long deadline = System.currentTimeMillis() + TIMEOUT_MS;
    while (System.currentTimeMillis() < deadline) {
      Event event = publisher.pollEvent();
      if (event != null) {
        return event;
      }
    }
    throw new AssertionError("no event was received");
  }

  /**
   * An indexer evicted from its group (no poll within max.poll.interval.ms, here because a slow send holds up the batch
   * waiting for a slot) must not send the batch it dispatches afterwards: the partition may already belong to another
   * consumer, which may since have deleted the document. The batch is skipped instead, and re-polled and sent once the
   * consumer rejoins. Before the ownership check, it was sent by the evicted consumer and then again after the re-poll.
   */
  @Test
  public void testEvictedIndexerDoesNotSendBatchDispatchedAfterEviction() throws Exception {
    String pipeline = newPipeline(1);
    // One document per batch, never flushed by time; a 5 s poll deadline. Long enough that a loaded run does not
    // repeatedly lose ownership of a batch it is still allowed to send (which re-polls and re-sends it).
    Config config = config(pipeline, 2, Map.of("indexer.batchSize", 1, "indexer.batchTimeoutMs", 600000,
        "kafka.pollIntervalMs", 100, "kafka.maxPollIntervalMs", 5000));
    String topic = KafkaUtils.getDestTopicName(pipeline);
    TopicPartition partition = new TopicPartition(topic, 0);
    // A fifth document, so the fourth is flushed (a batch of one is flushed by the next add).
    List<String> ids = produce(config, pipeline, 0, 5, "run");

    KafkaIndexerMessenger messenger = new KafkaIndexerMessenger(config, pipeline);
    GatedIndexer indexer = new GatedIndexer(config.getConfig("indexer"), messenger, ids.get(0));
    Thread thread = new Thread(indexer);
    thread.start();
    try {
      // Polling doc1 dispatches doc0, whose send is held. Polling doc2 dispatches doc1. Polling doc3 dispatches doc2,
      // which waits for doc0 (the window is full), so the indexer stops polling and is evicted.
      waitFor(() -> indexer.gatedSendStarted);
      // Wait for the messenger's own poll deadline to pass, not for Kafka to drop the consumer from its group: that
      // deadline is what stops ownership being confirmed, and Kafka's heartbeat-driven leave is not timely on every
      // platform. With no KafkaDocument, ownsInput() reports only the deadline.
      Document probe = Document.create("probe");
      waitFor(() -> !messenger.ownsInput(probe));
      // doc2 is dispatched only now, after the eviction.
      indexer.release();
      // At least 4: docs 0 to 3 are committed. The offset can move past 4 (doc 4 is dispatched too), so equality is not
      // the condition.
      waitFor(() -> {
        Long offset = committed(config).get(partition);
        return offset != null && offset >= 4L;
      }, () -> "sent=" + indexer.sent + " committed=" + committed(config));
    } finally {
      indexer.release();
      indexer.terminate();
      thread.join(TIMEOUT_MS);
    }

    // A document may be sent more than once: a send that started before the eviction is not recalled, and a document
    // whose send was skipped is re-polled and sent again after the consumer rejoins. What must never happen is a send
    // that begins once the messenger has stopped confirming ownership. Checking that on entry to each send does not
    // depend on whether doc2 or doc3 happened to be dispatched before or after the deadline.
    assertEquals(indexer.sent.toString(), List.of(), indexer.sentWhileUnowned);
    assertTrue(indexer.sent.containsAll(ids.subList(0, 4)));
  }

  // --- helpers ---

  private static String indexerGroup(Config config) {
    return config.getString("kafka.consumerGroupId") + "-indexer";
  }

  private static KafkaConsumer<String, String> groupMember(Config config) {
    return groupMember(config, indexerGroup(config));
  }

  private static KafkaConsumer<String, String> groupMember(Config config, String group) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrapServers"));
    props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    return new KafkaConsumer<>(props);
  }

  private static GroupState groupState(Config config) throws Exception {
    String group = indexerGroup(config);
    return admin.describeConsumerGroups(List.of(group)).all().get().get(group).groupState();
  }

  private static String newPipeline(int partitions) {
    String pipeline = "e2e" + counter.incrementAndGet();
    embeddedKafka.addTopics(new NewTopic(KafkaUtils.getDestTopicName(pipeline), partitions, (short) 1));
    return pipeline;
  }

  private static Config config(String pipeline, int maxConcurrentBatches, Map<String, Object> extra) {
    Map<String, Object> settings = new HashMap<>(Map.of(
        "kafka.bootstrapServers", embeddedKafka.getBrokersAsString(),
        "kafka.consumerGroupId", "group-" + pipeline,
        "kafka.maxPollIntervalMs", 300000,
        "kafka.maxRequestSize", 1048576,
        "kafka.events", false,
        "indexer.maxConcurrentBatches", maxConcurrentBatches));
    settings.putAll(extra);
    return ConfigFactory.parseMap(settings);
  }

  private List<String> produce(Config config, String pipeline, int partition, int count, String runId)
      throws Exception {
    if (producer == null) {
      producer = KafkaUtils.createDocumentProducer(config);
    }
    List<String> ids = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      String id = "p" + partition + "-" + counter.incrementAndGet();
      producer.send(new ProducerRecord<>(
          KafkaUtils.getDestTopicName(pipeline), partition, id, Document.create(id, runId))).get();
      ids.add(id);
    }
    return ids;
  }

  private static Map<TopicPartition, Long> committed(Config config) {
    try {
      Map<TopicPartition, OffsetAndMetadata> offsets = admin
          .listConsumerGroupOffsets(indexerGroup(config))
          .partitionsToOffsetAndMetadata().get();
      Map<TopicPartition, Long> result = new HashMap<>();
      offsets.forEach((partition, offset) -> {
        if (offset != null) {
          result.put(partition, offset.offset());
        }
      });
      return result;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void waitFor(BooleanSupplier condition) throws InterruptedException {
    waitFor(condition, () -> "");
  }

  private static void waitFor(BooleanSupplier condition, Supplier<String> detail) throws InterruptedException {
    long deadline = System.currentTimeMillis() + TIMEOUT_MS;
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("condition not met within " + TIMEOUT_MS + " ms " + detail.get());
      }
      Thread.sleep(20);
    }
  }

  private static KafkaConsumer<String, String> eventConsumer(Config config) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrapServers"));
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "events-" + counter.incrementAndGet());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    return new KafkaConsumer<>(props);
  }

  // An indexer that records the id of every document it sends, in send order.
  public static class RecordingIndexer extends Indexer {

    public static final Spec SPEC = SpecBuilder.indexer().build();

    final List<String> sent = Collections.synchronizedList(new ArrayList<>());

    RecordingIndexer(Config config, IndexerMessenger messenger) {
      super(config, new IndexerContext(messenger, false, "KafkaConcurrentIndexerEndToEndTest", "KafkaConcurrentIndexerEndToEndTest"));
    }

    @Override
    protected String getIndexerConfigKey() {
      return null;
    }

    @Override
    protected boolean checkConnection() {
      return true;
    }

    @Override
    public void closeConnection() {
    }

    @Override
    protected List<IndexerFailure> sendToIndex(PendingSend send) {
      List<Document> documents = send.documents();
      documents.forEach(doc -> sent.add(doc.getId()));
      return List.of();
    }
  }

  // A concurrent RecordingIndexer whose send of one chosen document is held until released.
  public static class GatedIndexer extends RecordingIndexer {

    public static final Spec SPEC = SpecBuilder.indexer().build();

    volatile boolean gatedSendStarted;
    // Ids of documents whose send began while the messenger no longer owned their input (checked on entry to sendToIndex).
    final List<String> sentWhileUnowned = Collections.synchronizedList(new ArrayList<>());
    private final IndexerMessenger messenger;
    private final String gatedId;
    private final CountDownLatch gate = new CountDownLatch(1);

    GatedIndexer(Config config, IndexerMessenger messenger, String gatedId) {
      super(config, messenger);
      this.messenger = messenger;
      this.gatedId = gatedId;
    }

    void release() {
      gate.countDown();
    }

    @Override
    protected boolean supportsConcurrentSends() {
      return true;
    }

    @Override
    protected List<IndexerFailure> sendToIndex(PendingSend send) {
      List<Document> documents = send.documents();
      for (Document doc : documents) {
        if (!messenger.ownsInput(doc)) {
          sentWhileUnowned.add(doc.getId());
        }
      }
      if (documents.stream().anyMatch(doc -> doc.getId().equals(gatedId))) {
        gatedSendStarted = true;
        try {
          gate.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
      }
      return super.sendToIndex(send);
    }
  }

  // An indexer that sends batches concurrently, each send taking a random few milliseconds (a stand-in for a slow
  // destination), while recording the peak number of sends running at once.
  public static class SlowIndexer extends Indexer {

    public static final Spec SPEC = SpecBuilder.indexer().build();

    final AtomicInteger peakConcurrent = new AtomicInteger();
    private final AtomicInteger concurrent = new AtomicInteger();

    SlowIndexer(Config config, IndexerMessenger messenger) {
      super(config, new IndexerContext(messenger, false, "KafkaConcurrentIndexerEndToEndTest", "KafkaConcurrentIndexerEndToEndTest"));
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
    protected boolean checkConnection() {
      return true;
    }

    @Override
    public void closeConnection() {
    }

    @Override
    protected List<IndexerFailure> sendToIndex(PendingSend send) throws IndexerException {
      List<Document> documents = send.documents();
      peakConcurrent.accumulateAndGet(concurrent.incrementAndGet(), Math::max);
      try {
        Thread.sleep(ThreadLocalRandom.current().nextInt(20, 120));
        return List.of();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IndexerException("Interrupted while sending", e);
      } finally {
        concurrent.decrementAndGet();
      }
    }
  }

  // A concurrent indexer over a simulated destination: each document carries destId (its destination id, via
  // idOverrideField), op ("add" or "delete") and version. A send takes a few milliseconds, then applies its documents to
  // the destination in order, recording per destination id the order operations were applied.
  public static class DestinationIndexer extends Indexer {

    public static final Spec SPEC = SpecBuilder.indexer().build();

    final AtomicInteger peakConcurrent = new AtomicInteger();
    private final AtomicInteger concurrent = new AtomicInteger();
    private final Map<String, String> destination = new HashMap<>();
    private final Map<String, List<String>> applied = new HashMap<>();

    DestinationIndexer(Config config, IndexerMessenger messenger) {
      super(config, new IndexerContext(messenger, false, "KafkaConcurrentIndexerEndToEndTest", "KafkaConcurrentIndexerEndToEndTest"));
    }

    Map<String, String> content() {
      synchronized (destination) {
        return new HashMap<>(destination);
      }
    }

    Map<String, List<String>> appliedOrder() {
      synchronized (destination) {
        Map<String, List<String>> copy = new HashMap<>();
        applied.forEach((k, v) -> copy.put(k, new ArrayList<>(v)));
        return copy;
      }
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
    protected boolean checkConnection() {
      return true;
    }

    @Override
    public void closeConnection() {
    }

    @Override
    protected List<IndexerFailure> sendToIndex(PendingSend send) throws IndexerException {
      List<Document> documents = send.documents();
      peakConcurrent.accumulateAndGet(concurrent.incrementAndGet(), Math::max);
      try {
        Thread.sleep(ThreadLocalRandom.current().nextInt(1, 4));
        synchronized (destination) {
          for (Document doc : documents) {
            String destId = doc.getString("destId");
            if (doc.getString("op").equals("add")) {
              destination.put(destId, doc.getString("version"));
            } else {
              destination.remove(destId);
            }
            applied.computeIfAbsent(destId, k -> new ArrayList<>()).add(doc.getId());
          }
        }
        return List.of();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IndexerException("Interrupted while sending", e);
      } finally {
        concurrent.decrementAndGet();
      }
    }
  }
}
