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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
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
 * Runs a real Indexer at indexer.maxConcurrentBatches 3 over KafkaIndexerMessenger and an embedded broker: every document
 * gets a FINISH event, and the committed offsets end after the last record of each partition.
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
    embeddedKafka.addTopics(new NewTopic(topic, 2, (short) 1));
    Config config = ConfigFactory.parseMap(Map.of(
        "kafka.bootstrapServers", embeddedKafka.getBrokersAsString(),
        "kafka.consumerGroupId", "group-" + pipeline,
        "kafka.maxPollIntervalSecs", 300,
        "kafka.maxRequestSize", 1048576,
        "kafka.events", true,
        "indexer.maxConcurrentBatches", 3,
        "indexer.batchSize", 2,
        "indexer.batchTimeout", 20));

    int perPartition = 15;
    Set<String> ids = new HashSet<>();
    try (KafkaProducer<String, Document> producer = KafkaUtils.createDocumentProducer(config)) {
      for (int partition = 0; partition < 2; partition++) {
        for (int i = 0; i < perPartition; i++) {
          String id = "p" + partition + "-" + i;
          producer.send(new ProducerRecord<>(topic, partition, id, Document.create(id, runId))).get();
          ids.add(id);
        }
      }
    }
    Map<TopicPartition, Long> expected = Map.of(
        new TopicPartition(topic, 0), (long) perPartition, new TopicPartition(topic, 1), (long) perPartition);

    SlowIndexer indexer = new SlowIndexer(config, new KafkaIndexerMessenger(config, pipeline));
    Thread thread = new Thread(indexer);
    thread.start();
    try {
      long deadline = System.currentTimeMillis() + TIMEOUT_MS;
      while (!expected.equals(committed(config)) && System.currentTimeMillis() < deadline) {
        Thread.sleep(50);
      }
    } finally {
      indexer.terminate();
      thread.join(TIMEOUT_MS);
    }
    assertEquals(expected, committed(config));
    assertTrue("batches were sent concurrently", indexer.maxConcurrent.get() > 1);

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

  // Indexer that sends batches concurrently, taking a random few milliseconds per batch.
  public static class SlowIndexer extends Indexer {

    public static final Spec SPEC = SpecBuilder.indexer().build();

    final AtomicInteger maxConcurrent = new AtomicInteger();
    private final AtomicInteger concurrent = new AtomicInteger();

    SlowIndexer(Config config, IndexerMessenger messenger) {
      super(config, messenger, false, "KafkaIndexerMessengerConcurrencyTest", null);
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
