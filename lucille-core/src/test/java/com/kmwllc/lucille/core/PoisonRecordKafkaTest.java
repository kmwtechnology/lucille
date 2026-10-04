package com.kmwllc.lucille.core;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.kmwllc.lucille.message.HybridWorkerMessenger;
import com.kmwllc.lucille.message.KafkaIndexerMessenger;
import com.kmwllc.lucille.message.KafkaUtils;
import com.kmwllc.lucille.message.KafkaWorkerMessenger;
import com.kmwllc.lucille.message.WorkerMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * Tests handling of a record that cannot be deserialized (a "poison" record) on the Worker and Indexer
 * consumers, under both settings of {@code kafka.onDeserializationError}.
 *
 * <p>Without handling, the consumer's position never advances past a poison record: every poll throws
 * {@code RecordDeserializationException} for it, so valid records behind it are never consumed.
 */
public class PoisonRecordKafkaTest {

  private static final byte[] POISON = new byte[]{0x00, 0x01, 0x02, 0x03};

  // This is a class-level Embedded instance of Kafka. Each test must use its own unique
  // topic names and consumer group ids to avoid conflicts. (Topic names are derived from
  // the pipeline name, so each test uses a unique pipeline name.)
  public static EmbeddedKafkaBroker embeddedKafka;

  @BeforeClass
  public static void startKafka() throws Exception {
    embeddedKafka = new EmbeddedKafkaKraftBroker(1, 1);
    embeddedKafka.afterPropertiesSet();
  }

  @AfterClass
  public static void stopKafka() {
    if (embeddedKafka != null) {
      embeddedKafka.destroy();
    }
  }

  private static Config buildConfig(String pipelineName, String groupId, String extraKafka) {
    return ConfigFactory.parseString(String.format(
        "kafka {\n"
            + "  bootstrapServers: \"%s\"\n"
            + "  consumerGroupId: \"%s\"\n"
            + "  maxPollIntervalSecs: 30\n"
            + "  maxRequestSize: 10000000\n"
            + "  %s\n"
            + "}\n"
            + "pipelines: [{name: \"%s\", stages: [{class: \"com.kmwllc.lucille.stage.NopStage\"}]}]\n",
        embeddedKafka.getBrokersAsString(), groupId, extraKafka, pipelineName));
  }

  /**
   * Produces the given records to a single-partition topic. A null value produces a poison record keyed by
   * that id; otherwise the value is a valid serialized Document.
   */
  private void produce(String topic, String... ids) throws Exception {
    embeddedKafka.addTopics(new NewTopic(topic, 1, (short) 1));
    Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);

    try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps)) {
      for (String id : ids) {
        byte[] value = id.startsWith("poison")
            ? POISON
            : Document.create(id, "run1").toString().getBytes(StandardCharsets.UTF_8);
        producer.send(new ProducerRecord<>(topic, id, value)).get();
      }
    }
  }

  /** Polls until a document is returned, collecting any nulls along the way. */
  private static Document pollUntilDoc(PollFn poll) throws Exception {
    long deadline = System.currentTimeMillis() + 60000;
    while (System.currentTimeMillis() < deadline) {
      Document doc = poll.poll();
      if (doc != null) {
        return doc;
      }
    }
    fail("no document polled within 60s");
    return null;
  }

  /** Polls until the poll throws a RecordDeserializationException. */
  private static RecordDeserializationException pollUntilPoison(PollFn poll) throws Exception {
    long deadline = System.currentTimeMillis() + 60000;
    while (System.currentTimeMillis() < deadline) {
      try {
        assertNull("no document should be returned past the poison record", poll.poll());
      } catch (RecordDeserializationException e) {
        return e;
      }
    }
    fail("poll did not throw RecordDeserializationException within 60s");
    return null;
  }

  private interface PollFn {
    Document poll() throws Exception;
  }

  private static List<ConsumerRecord<String, byte[]>> readAll(String topic, int expected) {
    Map<String, Object> consumerProps =
        KafkaTestUtils.consumerProps(embeddedKafka, topic + "_inspector", false);
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    DefaultKafkaConsumerFactory<String, byte[]> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
    List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
    try (Consumer<String, byte[]> inspector = cf.createConsumer()) {
      TopicPartition tp = new TopicPartition(topic, 0);
      inspector.assign(List.of(tp));
      inspector.seekToBeginning(List.of(tp));
      long deadline = System.currentTimeMillis() + 30000;
      while (records.size() < expected && System.currentTimeMillis() < deadline) {
        inspector.poll(Duration.ofMillis(500)).forEach(records::add);
      }
    }
    return records;
  }

  private static List<Event> readEvents(String topic, int expected) throws Exception {
    List<Event> events = new ArrayList<>();
    for (ConsumerRecord<String, byte[]> record : readAll(topic, expected)) {
      events.add(Event.fromJsonString(new String(record.value(), StandardCharsets.UTF_8)));
    }
    return events;
  }

  private static long committedOffset(Config config, String groupId, String topic) throws Exception {
    Properties adminProps = new Properties();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrapServers"));
    try (Admin admin = Admin.create(adminProps)) {
      Map<TopicPartition, OffsetAndMetadata> committed =
          admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
      OffsetAndMetadata offset = committed.get(new TopicPartition(topic, 0));
      assertNotNull("no committed offset for " + topic, offset);
      return offset.offset();
    }
  }

  /**
   * Default (fail) mode: the poll throws for the poison record instead of skipping it, and a FAIL event
   * naming the record is sent, keyed by its document id and attributed to the run of the last good document.
   */
  @Test(timeout = 120000)
  public void testFailModeStopsAtPoisonRecord() throws Exception {
    String pipelineName = "poison_fail";
    Config config = buildConfig(pipelineName, "poison_fail_group", "");
    produce(KafkaUtils.getSourceTopicName(pipelineName, config), "doc1", "poison2", "doc3");

    KafkaWorkerMessenger messenger = new KafkaWorkerMessenger(config, pipelineName);
    try {
      assertEquals("doc1", pollUntilDoc(messenger::pollDocToProcess).getId());

      RecordDeserializationException poison = pollUntilPoison(messenger::pollDocToProcess);
      assertEquals(1L, poison.offset());

      // the position was not advanced: the same record fails again
      RecordDeserializationException again =
          assertThrows(RecordDeserializationException.class, messenger::pollDocToProcess);
      assertEquals(1L, again.offset());
    } finally {
      messenger.close();
    }

    List<Event> events = readEvents(KafkaUtils.getEventTopicName(config, pipelineName, "run1"), 2);
    assertEquals(2, events.size());
    for (Event event : events) {
      assertEquals("poison2", event.getDocumentId());
      assertEquals(Event.Type.FAIL, event.getType());
    }
  }

  /**
   * Default (fail) mode with a real Worker: the worker stops on the poison record, closes its messenger,
   * and the group's committed offset stays before the poison record.
   */
  @Test(timeout = 180000)
  public void testFailModeStopsWorker() throws Exception {
    String pipelineName = "poison_fail_worker";
    String groupId = "poison_fail_worker_group";
    Config config = buildConfig(pipelineName, groupId, "");
    String sourceTopic = KafkaUtils.getSourceTopicName(pipelineName, config);
    produce(sourceTopic, "doc1", "poison2", "doc3");

    RecordingWorkerMessenger messenger =
        new RecordingWorkerMessenger(new KafkaWorkerMessenger(config, pipelineName));
    Worker worker = new Worker(config, messenger, "run1", pipelineName, pipelineName);
    WorkerThread workerThread = Worker.startThread(worker, "poison-fail-worker");

    workerThread.join(120000);
    assertFalse("worker should stop on the poison record", workerThread.isAlive());
    assertTrue(messenger.lastPollFailure instanceof RecordDeserializationException);
    assertTrue("worker should close its messenger on the way out", messenger.closed);

    assertEquals(1L, committedOffset(config, groupId, sourceTopic));
    List<ConsumerRecord<String, byte[]>> indexed = readAll(KafkaUtils.getDestTopicName(pipelineName), 1);
    assertEquals(1, indexed.size());
    assertEquals("doc1", indexed.get(0).key());
  }

  /**
   * Skip mode with a real Worker: the poison record is copied verbatim to the fail topic, a FAIL event is
   * sent, the documents on either side of it are processed, the committed offset moves past all three
   * records, and the worker keeps running.
   */
  @Test(timeout = 180000)
  public void testSkipModeDeadLettersAndContinues() throws Exception {
    String pipelineName = "poison_skip";
    String groupId = "poison_skip_group";
    Config config = buildConfig(pipelineName, groupId, "onDeserializationError: skip");
    String sourceTopic = KafkaUtils.getSourceTopicName(pipelineName, config);
    produce(sourceTopic, "doc1", "poison2", "doc3");

    Worker worker = new Worker(config, new KafkaWorkerMessenger(config, pipelineName), "run1", pipelineName,
        pipelineName);
    WorkerThread workerThread = Worker.startThread(worker, "poison-skip-worker");
    try {
      List<ConsumerRecord<String, byte[]>> indexed = readAll(KafkaUtils.getDestTopicName(pipelineName), 2);
      assertEquals(List.of("doc1", "doc3"), indexed.stream().map(ConsumerRecord::key).toList());
      assertTrue("worker should still be running", workerThread.isAlive());
    } finally {
      workerThread.terminate();
      workerThread.join(60000);
    }

    assertEquals(3L, committedOffset(config, groupId, sourceTopic));

    List<ConsumerRecord<String, byte[]>> failed = readAll(KafkaUtils.getFailTopicName(pipelineName), 1);
    assertEquals(1, failed.size());
    assertEquals("poison2", failed.get(0).key());
    assertArrayEquals(POISON, failed.get(0).value());

    List<Event> events = readEvents(KafkaUtils.getEventTopicName(config, pipelineName, "run1"), 1);
    assertEquals(1, events.size());
    assertEquals("poison2", events.get(0).getDocumentId());
    assertEquals(Event.Type.FAIL, events.get(0).getType());
  }

  /**
   * Skip mode escalates to fail once maxConsecutiveDeserializationErrors poison records arrive in a row on
   * one partition, and a good record in between resets the count.
   */
  @Test(timeout = 120000)
  public void testSkipModeEscalatesOnConsecutivePoison() throws Exception {
    String pipelineName = "poison_streak";
    Config config = buildConfig(pipelineName, "poison_streak_group",
        "onDeserializationError: skip\n  maxConsecutiveDeserializationErrors: 2");
    produce(KafkaUtils.getSourceTopicName(pipelineName, config),
        "doc1", "poison2", "doc3", "poison4", "poison5", "doc6");

    KafkaWorkerMessenger messenger = new KafkaWorkerMessenger(config, pipelineName);
    try {
      assertEquals("doc1", pollUntilDoc(messenger::pollDocToProcess).getId());
      // poison2 is skipped; doc3 resets the streak
      assertEquals("doc3", pollUntilDoc(messenger::pollDocToProcess).getId());
      // poison4 is skipped, poison5 is the second in a row and escalates
      RecordDeserializationException e = pollUntilPoison(messenger::pollDocToProcess);
      assertEquals(4L, e.offset());
    } finally {
      messenger.close();
    }
  }

  /** Skip mode on the indexer's consumer: the poison record is skipped and the position committed past it. */
  @Test(timeout = 120000)
  public void testSkipModeOnIndexerMessenger() throws Exception {
    String pipelineName = "poison_indexer";
    String groupId = "poison_indexer_group";
    Config config = buildConfig(pipelineName, groupId, "onDeserializationError: skip");
    String destTopic = KafkaUtils.getDestTopicName(pipelineName);
    produce(destTopic, "doc1", "poison2", "doc3");

    KafkaIndexerMessenger messenger = new KafkaIndexerMessenger(config, pipelineName);
    try {
      assertEquals("doc1", pollUntilDoc(messenger::pollDocToIndex).getId());
      assertEquals("doc3", pollUntilDoc(messenger::pollDocToIndex).getId());
    } finally {
      messenger.close();
    }

    assertTrue(committedOffset(config, groupId, destTopic) >= 2L);
    List<ConsumerRecord<String, byte[]>> failed = readAll(KafkaUtils.getFailTopicName(pipelineName), 1);
    assertEquals(1, failed.size());
    assertArrayEquals(POISON, failed.get(0).value());
  }

  /** Skip mode on the hybrid worker's consumer: the poison record is dead-lettered and skipped. */
  @Test(timeout = 120000)
  public void testSkipModeOnHybridWorkerMessenger() throws Exception {
    String pipelineName = "poison_hybrid";
    Config config = buildConfig(pipelineName, "poison_hybrid_group", "onDeserializationError: skip");
    produce(KafkaUtils.getSourceTopicName(pipelineName, config), "doc1", "poison2", "doc3");

    HybridWorkerMessenger messenger = new HybridWorkerMessenger(config, pipelineName,
        new LinkedBlockingQueue<>(), new LinkedBlockingQueue<>());
    try {
      assertEquals("doc1", pollUntilDoc(messenger::pollDocToProcess).getId());
      assertEquals("doc3", pollUntilDoc(messenger::pollDocToProcess).getId());
    } finally {
      messenger.close();
    }

    List<ConsumerRecord<String, byte[]>> failed = readAll(KafkaUtils.getFailTopicName(pipelineName), 1);
    assertEquals(1, failed.size());
    assertEquals("poison2", failed.get(0).key());
    assertArrayEquals(POISON, failed.get(0).value());
  }

  @Test
  public void testInvalidModeRejected() {
    Config config = buildConfig("poison_invalid", "poison_invalid_group", "onDeserializationError: ignore");
    assertThrows(IllegalArgumentException.class, () -> new KafkaWorkerMessenger(config, "poison_invalid"));
  }

  /**
   * Delegating messenger that records the exception (if any) thrown by pollDocToProcess and whether it was
   * closed, so the test can verify how the worker stopped.
   */
  private static class RecordingWorkerMessenger implements WorkerMessenger {

    private final WorkerMessenger delegate;
    private volatile Throwable lastPollFailure;
    private volatile boolean closed;

    RecordingWorkerMessenger(WorkerMessenger delegate) {
      this.delegate = delegate;
    }

    @Override
    public Document pollDocToProcess() throws Exception {
      try {
        return delegate.pollDocToProcess();
      } catch (Exception e) {
        lastPollFailure = e;
        throw e;
      }
    }

    @Override
    public void commitPendingDocOffsets() throws Exception {
      delegate.commitPendingDocOffsets();
    }

    @Override
    public void sendForIndexing(Document document) throws Exception {
      delegate.sendForIndexing(document);
    }

    @Override
    public void sendFailed(Document document) throws Exception {
      delegate.sendFailed(document);
    }

    @Override
    public void sendEvent(Document document, String message, Event.Type type) throws Exception {
      delegate.sendEvent(document, message, type);
    }

    @Override
    public void sendEvent(Event event) throws Exception {
      delegate.sendEvent(event);
    }

    @Override
    public void close() throws Exception {
      closed = true;
      delegate.close();
    }
  }
}
