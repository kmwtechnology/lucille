package com.kmwllc.lucille.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.KafkaDocument;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

/**
 * Tests how KafkaIndexerMessenger commits destination-topic offsets: at poll when indexer.maxConcurrentBatches is 1, and
 * on batch completion (at-least-once) when it is greater than 1, along with keepAlive() and the shortened idle poll.
 * Most tests run against an embedded broker; a few use MockConsumer to drive rebalances deterministically.
 */
public class KafkaIndexerMessengerCommitTest {

  private static final long TIMEOUT_MS = 30000;
  private static final AtomicInteger counter = new AtomicInteger();

  private static EmbeddedKafkaBroker embeddedKafka;
  private static Admin admin;

  private final List<KafkaIndexerMessenger> messengers = new ArrayList<>();
  private KafkaProducer<String, Document> producer;

  @BeforeClass
  public static void startKafka() {
    embeddedKafka = new EmbeddedKafkaKraftBroker(1, 1);
    embeddedKafka.afterPropertiesSet();
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
    for (KafkaIndexerMessenger messenger : messengers) {
      messenger.close();
    }
    if (producer != null) {
      producer.close();
    }
  }

  @Test
  public void testSingleBatchCommitsAtPoll() throws Exception {
    String pipeline = newPipeline(1);
    Config config = config(pipeline, 1);
    produce(config, pipeline, 0, 3);
    KafkaIndexerMessenger messenger = messenger(config, pipeline);
    TopicPartition p0 = new TopicPartition(KafkaUtils.getDestTopicName(pipeline), 0);

    KafkaDocument first = pollDoc(messenger);
    assertEquals(0, first.getOffset());
    // committed as soon as it is polled, before batchComplete
    assertEquals(Long.valueOf(1), committed(config).get(p0));

    messenger.batchComplete(List.of(first));
    assertEquals(Long.valueOf(1), committed(config).get(p0));

    pollDoc(messenger);
    assertEquals(Long.valueOf(2), committed(config).get(p0));
  }

  @Test
  public void testConcurrentCommitsOnCompletionNotAtPoll() throws Exception {
    String pipeline = newPipeline(1);
    Config config = config(pipeline, 3);
    produce(config, pipeline, 0, 3);
    KafkaIndexerMessenger messenger = messenger(config, pipeline);
    TopicPartition p0 = new TopicPartition(KafkaUtils.getDestTopicName(pipeline), 0);

    List<Document> docs = List.of(pollDoc(messenger), pollDoc(messenger), pollDoc(messenger));
    assertTrue(committed(config).isEmpty());

    messenger.batchComplete(docs.subList(0, 2));
    assertEquals(Long.valueOf(2), committed(config).get(p0));

    messenger.batchComplete(docs.subList(2, 3));
    assertEquals(Long.valueOf(3), committed(config).get(p0));
  }

  @Test
  public void testConcurrentCommitsLastOffsetPerPartition() throws Exception {
    String pipeline = newPipeline(2);
    Config config = config(pipeline, 3);
    produce(config, pipeline, 0, 3);
    produce(config, pipeline, 1, 2);
    KafkaIndexerMessenger messenger = messenger(config, pipeline);
    String topic = KafkaUtils.getDestTopicName(pipeline);

    List<Document> docs = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      docs.add(pollDoc(messenger));
    }
    assertTrue(committed(config).isEmpty());

    messenger.batchComplete(docs);
    Map<TopicPartition, Long> committed = committed(config);
    assertEquals(Long.valueOf(3), committed.get(new TopicPartition(topic, 0)));
    assertEquals(Long.valueOf(2), committed.get(new TopicPartition(topic, 1)));
  }

  @Test
  public void testConcurrentCommitsOnlyAssignedPartitions() throws Exception {
    String pipeline = newPipeline(2);
    // cooperative rebalancing lets messenger A keep one partition while the other moves to B
    Config config = config(pipeline, 3, Map.of(
        "kafka.consumer.partition.assignment.strategy", CooperativeStickyAssignor.class.getName()));
    produce(config, pipeline, 0, 3);
    produce(config, pipeline, 1, 2);
    String topic = KafkaUtils.getDestTopicName(pipeline);

    KafkaIndexerMessenger a = messenger(config, pipeline);
    List<Document> docs = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      docs.add(pollDoc(a));
    }

    KafkaIndexerMessenger b = messenger(config, pipeline);
    waitFor(() -> {
      try {
        a.keepAlive();
        b.keepAlive();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return a.assignment().size() == 1 && b.assignment().size() == 1;
    });
    TopicPartition kept = a.assignment().iterator().next();
    TopicPartition moved = b.assignment().iterator().next();

    a.batchComplete(docs);
    Map<TopicPartition, Long> committed = committed(config);
    assertEquals(Long.valueOf(kept.partition() == 0 ? 3 : 2), committed.get(kept));
    // the revoked partition's offsets are dropped, so B redelivers its records
    assertNull(committed.get(moved));
    assertEquals(moved.partition(), pollDoc(b).getPartition());
  }

  @Test
  public void testKeepAliveDeliversAndSkipsNothing() throws Exception {
    String pipeline = newPipeline(1);
    Config config = config(pipeline, 3);
    produce(config, pipeline, 0, 3);
    KafkaIndexerMessenger messenger = messenger(config, pipeline);

    assertEquals(0, pollDoc(messenger).getOffset());
    for (int i = 0; i < 20; i++) {
      messenger.keepAlive();
      if (i == 5) {
        produce(config, pipeline, 0, 3);
      }
      Thread.sleep(10);
    }
    assertTrue(committed(config).isEmpty());
    for (long offset = 1; offset < 6; offset++) {
      assertEquals(offset, pollDoc(messenger).getOffset());
    }
    assertNull(messenger.pollDocToIndex());
  }

  @Test
  public void testKeepAliveWhileJoiningGroupSkipsNothing() throws Exception {
    String pipeline = newPipeline(1);
    Config config = config(pipeline, 3);
    produce(config, pipeline, 0, 5);
    KafkaIndexerMessenger messenger = messenger(config, pipeline);

    // the partition is assigned during one of these polls, unpaused, and may return a record
    waitFor(() -> {
      try {
        messenger.keepAlive();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return !messenger.assignment().isEmpty();
    });
    for (int i = 0; i < 20; i++) {
      messenger.keepAlive();
      Thread.sleep(10);
    }
    for (long offset = 0; offset < 5; offset++) {
      assertEquals(offset, pollDoc(messenger).getOffset());
    }
  }

  @Test
  public void testKeepAliveSeeksBackOnNewlyAssignedPartition() throws Exception {
    MockConsumer<String, KafkaDocument> consumer = new MockConsumer<>("earliest");
    consumer.setMaxPollRecords(1);
    TopicPartition p0 = new TopicPartition("mock_dest", 0);
    KafkaIndexerMessenger messenger = new KafkaIndexerMessenger(mockConfig(3), "mock", consumer);

    // the partition is assigned during keepAlive's poll, after the assigned partitions were paused
    consumer.schedulePollTask(() -> {
      consumer.rebalance(List.of(p0));
      consumer.updateBeginningOffsets(Map.of(p0, 0L));
      consumer.addRecord(record(p0, 0));
    });
    messenger.keepAlive();
    assertEquals(0, consumer.position(p0));
    messenger.close();
  }

  @Test
  public void testRevokedAndReassignedPartitionIsNotRewound() throws Exception {
    MockConsumer<String, KafkaDocument> consumer = new MockConsumer<>("earliest");
    consumer.setMaxPollRecords(1);
    TopicPartition p0 = new TopicPartition("mock_dest", 0);
    TopicPartition p1 = new TopicPartition("mock_dest", 1);
    KafkaIndexerMessenger messenger = new KafkaIndexerMessenger(mockConfig(3), "mock", consumer);
    consumer.rebalance(List.of(p0, p1));
    consumer.updateBeginningOffsets(Map.of(p0, 0L, p1, 0L));

    consumer.addRecord(record(p0, 0));
    Document p0First = messenger.pollDocToIndex();
    consumer.addRecord(record(p0, 1));
    Document p0Second = messenger.pollDocToIndex();
    consumer.addRecord(record(p1, 0));
    Document p1First = messenger.pollDocToIndex();

    // p0 moves to another consumer, which commits further, then comes back
    consumer.rebalance(List.of(p1));
    consumer.commitSync(Map.of(p0, new OffsetAndMetadata(10)));
    consumer.rebalance(List.of(p0, p1));

    messenger.batchComplete(List.of(p0First, p0Second, p1First));
    assertEquals(10, consumer.committed(Set.of(p0)).get(p0).offset());
    assertEquals(1, consumer.committed(Set.of(p1)).get(p1).offset());

    // a record polled in the new assignment is committed normally
    consumer.seek(p0, 10);
    consumer.addRecord(record(p0, 10));
    messenger.batchComplete(List.of(messenger.pollDocToIndex()));
    assertEquals(11, consumer.committed(Set.of(p0)).get(p0).offset());
    messenger.close();
  }

  @Test
  public void testFailedCommitIsRetriedWithNextBatch() throws Exception {
    AtomicInteger failures = new AtomicInteger(1);
    MockConsumer<String, KafkaDocument> consumer = new MockConsumer<>("earliest") {
      @Override
      public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (failures.getAndDecrement() > 0) {
          throw new CommitFailedException("simulated");
        }
        super.commitSync(offsets);
      }
    };
    consumer.setMaxPollRecords(1);
    TopicPartition p0 = new TopicPartition("mock_dest", 0);
    TopicPartition p1 = new TopicPartition("mock_dest", 1);
    KafkaIndexerMessenger messenger = new KafkaIndexerMessenger(mockConfig(3), "mock", consumer);
    consumer.rebalance(List.of(p0, p1));
    consumer.updateBeginningOffsets(Map.of(p0, 0L, p1, 0L));
    consumer.addRecord(record(p1, 0));
    Document first = messenger.pollDocToIndex();
    consumer.addRecord(record(p0, 0));
    Document second = messenger.pollDocToIndex();

    assertThrows(CommitFailedException.class, () -> messenger.batchComplete(List.of(first)));
    assertNull(consumer.committed(Set.of(p1)).get(p1));

    messenger.batchComplete(List.of(second));
    Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(Set.of(p0, p1));
    assertEquals(1, committed.get(p0).offset());
    assertEquals(1, committed.get(p1).offset());
    messenger.close();
  }

  @Test
  public void testConcurrentIdlePollIsShort() throws Exception {
    String pipeline = newPipeline(1);
    Config config = config(pipeline, 3);
    produce(config, pipeline, 0, 1);
    KafkaIndexerMessenger messenger = messenger(config, pipeline);
    pollDoc(messenger);

    for (int i = 0; i < 5; i++) {
      long start = System.nanoTime();
      assertNull(messenger.pollDocToIndex());
      long elapsedMs = (System.nanoTime() - start) / 1_000_000;
      assertTrue("idle poll took " + elapsedMs + " ms", elapsedMs < 1000);
    }
  }

  @Test
  public void testConcurrentIndexerEndToEnd() throws Exception {
    String pipeline = newPipeline(2);
    String runId = "run-" + pipeline;
    Config config = config(pipeline, 3, Map.of("kafka.events", true, "indexer.batchSize", 2,
        "indexer.batchTimeout", 20));
    int perPartition = 15;
    Set<String> ids = new HashSet<>();
    ids.addAll(produce(config, pipeline, 0, perPartition, runId));
    ids.addAll(produce(config, pipeline, 1, perPartition, runId));
    String topic = KafkaUtils.getDestTopicName(pipeline);
    Map<TopicPartition, Long> expected = Map.of(
        new TopicPartition(topic, 0), (long) perPartition, new TopicPartition(topic, 1), (long) perPartition);

    KafkaIndexerMessenger messenger = new KafkaIndexerMessenger(config, pipeline);
    SlowIndexer indexer = new SlowIndexer(config, messenger);
    Thread thread = new Thread(indexer);
    thread.start();
    try {
      waitFor(() -> expected.equals(committed(config)));
    } finally {
      indexer.terminate();
      thread.join(TIMEOUT_MS);
    }
    assertEquals(expected, committed(config));
    assertTrue(indexer.maxConcurrent.get() > 1);

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

  // --- helpers ---

  private static String newPipeline(int partitions) {
    String pipeline = "commit" + counter.incrementAndGet();
    embeddedKafka.addTopics(new NewTopic(KafkaUtils.getDestTopicName(pipeline), partitions, (short) 1));
    return pipeline;
  }

  private static Config config(String pipeline, int maxConcurrentBatches) {
    return config(pipeline, maxConcurrentBatches, Map.of());
  }

  private static Config config(String pipeline, int maxConcurrentBatches, Map<String, Object> extra) {
    Map<String, Object> settings = new HashMap<>(Map.of(
        "kafka.bootstrapServers", embeddedKafka.getBrokersAsString(),
        "kafka.consumerGroupId", "group-" + pipeline,
        "kafka.maxPollIntervalSecs", 300,
        "kafka.maxRequestSize", 1048576,
        "kafka.events", false,
        "indexer.maxConcurrentBatches", maxConcurrentBatches));
    settings.putAll(extra);
    return ConfigFactory.parseMap(settings);
  }

  private static Config mockConfig(int maxConcurrentBatches) {
    return ConfigFactory.parseMap(Map.of(
        "kafka.bootstrapServers", "localhost:9092",
        "kafka.consumerGroupId", "mock",
        "kafka.maxPollIntervalSecs", 300,
        "kafka.maxRequestSize", 1048576,
        "kafka.events", false,
        "indexer.maxConcurrentBatches", maxConcurrentBatches));
  }

  private KafkaIndexerMessenger messenger(Config config, String pipeline) {
    KafkaIndexerMessenger messenger = new KafkaIndexerMessenger(config, pipeline);
    messengers.add(messenger);
    return messenger;
  }

  private void produce(Config config, String pipeline, int partition, int count) throws Exception {
    produce(config, pipeline, partition, count, "run");
  }

  private List<String> produce(Config config, String pipeline, int partition, int count, String runId) throws Exception {
    if (producer == null) {
      producer = KafkaUtils.createDocumentProducer(config);
    }
    List<String> ids = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      String id = "p" + partition + "-" + counter.incrementAndGet();
      producer.send(new ProducerRecord<>(KafkaUtils.getDestTopicName(pipeline), partition, id,
          Document.create(id, runId))).get();
      ids.add(id);
    }
    return ids;
  }

  private static KafkaDocument pollDoc(KafkaIndexerMessenger messenger) throws Exception {
    long deadline = System.currentTimeMillis() + TIMEOUT_MS;
    while (System.currentTimeMillis() < deadline) {
      Document doc = messenger.pollDocToIndex();
      if (doc != null) {
        return (KafkaDocument) doc;
      }
    }
    throw new AssertionError("no document polled within " + TIMEOUT_MS + " ms");
  }

  private static Map<TopicPartition, Long> committed(Config config) {
    try {
      Map<TopicPartition, OffsetAndMetadata> offsets = admin
          .listConsumerGroupOffsets(config.getString("kafka.consumerGroupId"))
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
    long deadline = System.currentTimeMillis() + TIMEOUT_MS;
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("condition not met within " + TIMEOUT_MS + " ms");
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

  private static ConsumerRecord<String, KafkaDocument> record(TopicPartition partition, long offset) {
    String id = "doc-" + partition.partition() + "-" + offset;
    try {
      KafkaDocument doc = new KafkaDocument(new ConsumerRecord<>(partition.topic(), partition.partition(), offset, id,
          Document.create(id, "run").toString()));
      return new ConsumerRecord<>(partition.topic(), partition.partition(), offset, id, doc);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Indexer that sends batches concurrently, taking a random few milliseconds per batch.
  public static class SlowIndexer extends Indexer {

    public static final Spec SPEC = SpecBuilder.indexer().build();

    final AtomicInteger maxConcurrent = new AtomicInteger();
    private final AtomicInteger concurrent = new AtomicInteger();

    SlowIndexer(Config config, IndexerMessenger messenger) {
      super(config, messenger, false, "KafkaIndexerMessengerCommitTest", null);
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
      maxConcurrent.accumulateAndGet(concurrent.incrementAndGet(), Math::max);
      try {
        Thread.sleep(ThreadLocalRandom.current().nextInt(20, 120));
        return Collections.emptySet();
      } finally {
        concurrent.decrementAndGet();
      }
    }
  }
}
