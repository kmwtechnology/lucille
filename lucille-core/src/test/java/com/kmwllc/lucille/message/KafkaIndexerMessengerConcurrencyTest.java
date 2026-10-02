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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

/**
 * Runs a real Indexer at indexer.maxConcurrentBatches 3 over KafkaIndexerMessenger and an embedded broker. While a batch
 * is held in flight, the committed offset does not cover it, even though later batches were polled and sent; once it is
 * released, every document gets a FINISH event and the committed offset ends after the last record.
 */
public class KafkaIndexerMessengerConcurrencyTest {

  private static final long TIMEOUT_MS = 30000;

  private static EmbeddedKafkaBroker embeddedKafka;
  private static Admin admin;

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

  @Test
  public void testConcurrentIndexerEndToEnd() throws Exception {
    String pipeline = "concurrent";
    String runId = "run-" + pipeline;
    String topic = KafkaUtils.getDestTopicName(pipeline);
    TopicPartition partition = new TopicPartition(topic, 0);
    embeddedKafka.addTopics(new NewTopic(topic, 1, (short) 1));
    Config config = ConfigFactory.parseMap(Map.of(
        "kafka.bootstrapServers", embeddedKafka.getBrokersAsString(),
        "kafka.consumerGroupId", "group-" + pipeline,
        "kafka.maxPollIntervalSecs", 300,
        "kafka.maxRequestSize", 1048576,
        "kafka.events", true,
        "indexer.maxConcurrentBatches", 3,
        "indexer.batchSize", 2,
        "indexer.batchTimeout", 10000));

    // Batches are [d0,d1] [d2,d3] [d4,d5] [d6,d7] [d8,d9]; [d2,d3] is held in its send.
    int count = 10;
    Set<String> ids = new HashSet<>();
    try (KafkaProducer<String, Document> producer = KafkaUtils.createDocumentProducer(config)) {
      for (int i = 0; i < count; i++) {
        String id = "d" + i;
        producer.send(new ProducerRecord<>(topic, 0, id, Document.create(id, runId))).get();
        ids.add(id);
      }
    }

    GatedIndexer indexer = new GatedIndexer(config, new KafkaIndexerMessenger(config, pipeline), "d2");
    Thread thread = new Thread(indexer);
    thread.start();
    try {
      // [d4,d5] and [d6,d7] have been polled and sent behind the held batch, which is still in flight.
      waitFor(() -> indexer.finished.containsAll(List.of("d4", "d6")));
      assertTrue("batches were sent concurrently", indexer.maxConcurrent.get() > 1);
      Long committed = committed(config).get(partition);
      assertTrue("committed " + committed + " covers the batch in flight", committed == null || committed <= 2);

      indexer.gate.countDown();
      waitFor(() -> Long.valueOf(count).equals(committed(config).get(partition)));
    } finally {
      indexer.gate.countDown();
      indexer.terminate();
      thread.join(TIMEOUT_MS);
    }
    assertEquals(Long.valueOf(count), committed(config).get(partition));

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

  private static void waitFor(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.currentTimeMillis() + TIMEOUT_MS;
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("condition not met within " + TIMEOUT_MS + " ms");
      }
      Thread.sleep(20);
    }
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

  private static KafkaConsumer<String, String> eventConsumer(Config config) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrapServers"));
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "events-" + config.getString("kafka.consumerGroupId"));
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    return new KafkaConsumer<>(props);
  }

  // Indexer that sends batches concurrently, holding the batch that contains heldId until gate is released.
  public static class GatedIndexer extends Indexer {

    public static final Spec SPEC = SpecBuilder.indexer().build();

    final CountDownLatch gate = new CountDownLatch(1);
    final Set<String> finished = ConcurrentHashMap.newKeySet();
    final AtomicInteger maxConcurrent = new AtomicInteger();
    private final AtomicInteger concurrent = new AtomicInteger();
    private final String heldId;

    GatedIndexer(Config config, IndexerMessenger messenger, String heldId) {
      super(config, messenger, false, "KafkaIndexerMessengerConcurrencyTest", null);
      this.heldId = heldId;
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
        if (documents.stream().anyMatch(d -> d.getId().equals(heldId))) {
          gate.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }
        return Collections.emptySet();
      } finally {
        concurrent.decrementAndGet();
        documents.forEach(d -> finished.add(d.getId()));
      }
    }
  }
}
