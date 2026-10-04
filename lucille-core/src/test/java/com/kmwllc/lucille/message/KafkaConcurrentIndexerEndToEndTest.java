package com.kmwllc.lucille.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.Indexer;
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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
  public void tearDown() {
    if (producer != null) {
      producer.close();
    }
  }

  @Test
  public void testConcurrentIndexerCommitsEverythingAndOverlapsSends() throws Exception {
    String pipeline = newPipeline(2);
    String runId = "run-" + pipeline;
    Config config = config(pipeline, 3, Map.of(
        "kafka.events", true, "indexer.batchSize", 2, "indexer.batchTimeout", 20));
    int perPartition = 15;

    Set<String> ids = new HashSet<>();
    ids.addAll(produce(config, pipeline, 0, perPartition, runId));
    ids.addAll(produce(config, pipeline, 1, perPartition, runId));

    String topic = KafkaUtils.getDestTopicName(pipeline);
    Map<TopicPartition, Long> expectedCommits = Map.of(
        new TopicPartition(topic, 0), (long) perPartition,
        new TopicPartition(topic, 1), (long) perPartition);

    KafkaIndexerMessenger messenger = new KafkaIndexerMessenger(config, pipeline);
    SlowIndexer indexer = new SlowIndexer(config, messenger);
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

  // --- helpers ---

  private static String newPipeline(int partitions) {
    String pipeline = "e2e" + counter.incrementAndGet();
    embeddedKafka.addTopics(new NewTopic(KafkaUtils.getDestTopicName(pipeline), partitions, (short) 1));
    return pipeline;
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

  // An indexer that sends batches concurrently, each send taking a random few milliseconds (a stand-in for a slow
  // destination), while recording the peak number of sends running at once.
  public static class SlowIndexer extends Indexer {

    public static final Spec SPEC = SpecBuilder.indexer().build();

    final AtomicInteger peakConcurrent = new AtomicInteger();
    private final AtomicInteger concurrent = new AtomicInteger();

    SlowIndexer(Config config, IndexerMessenger messenger) {
      super(config, messenger, false, "KafkaConcurrentIndexerEndToEndTest", "KafkaConcurrentIndexerEndToEndTest");
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
      peakConcurrent.accumulateAndGet(concurrent.incrementAndGet(), Math::max);
      try {
        Thread.sleep(ThreadLocalRandom.current().nextInt(20, 120));
        return Collections.emptySet();
      } finally {
        concurrent.decrementAndGet();
      }
    }
  }
}
