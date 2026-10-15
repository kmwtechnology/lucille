package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.message.KafkaUtils;
import com.kmwllc.lucille.message.KafkaWorkerMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
 * A standalone Worker against a real broker: source offsets are committed per batch of
 * {@code worker.commitBatchSize} documents, after a {@code worker.commitBatchTimeout} when the worker goes idle, and
 * on shutdown, and every document reaches the dest topic.
 */
public class KafkaWorkerBatchedCommitTest {

  // Each test uses its own pipeline name (and so its own topics) and consumer group.
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

  @Test(timeout = 180000)
  public void testCommitsPerBatchAndOnShutdown() throws Exception {
    String pipelineName = "batched_size";
    String groupId = "batched_size_group";
    Config config = buildConfig(pipelineName, groupId, 3, 600000);
    String sourceTopic = KafkaUtils.getSourceTopicName(pipelineName, config);
    produce(sourceTopic, 5);

    WorkerThread workerThread = Worker.startThread(
        new Worker(config, new KafkaWorkerMessenger(config, pipelineName), "run1", pipelineName, pipelineName),
        "batched-size-worker");
    try {
      assertEquals(5, read(KafkaUtils.getDestTopicName(pipelineName), 5).size());
      // the first batch of 3 is committed; the remaining 2 wait for a full batch, the timeout, or shutdown
      assertEquals(3L, waitForCommitted(config, groupId, sourceTopic, 3L));
      assertEquals(3L, committedOffset(config, groupId, sourceTopic));
    } finally {
      workerThread.terminate();
      workerThread.join(60000);
    }
    assertFalse(workerThread.isAlive());
    assertEquals(5L, committedOffset(config, groupId, sourceTopic));
  }

  @Test(timeout = 180000)
  public void testIdleWorkerCommitsAfterTimeout() throws Exception {
    String pipelineName = "batched_timeout";
    String groupId = "batched_timeout_group";
    Config config = buildConfig(pipelineName, groupId, 1000, 200);
    String sourceTopic = KafkaUtils.getSourceTopicName(pipelineName, config);
    produce(sourceTopic, 5);

    WorkerThread workerThread = Worker.startThread(
        new Worker(config, new KafkaWorkerMessenger(config, pipelineName), "run1", pipelineName, pipelineName),
        "batched-timeout-worker");
    try {
      assertEquals(5, read(KafkaUtils.getDestTopicName(pipelineName), 5).size());
      assertEquals(5L, waitForCommitted(config, groupId, sourceTopic, 5L));
      assertTrue("worker should still be running", workerThread.isAlive());
    } finally {
      workerThread.terminate();
      workerThread.join(60000);
    }
  }

  private static Config buildConfig(String pipelineName, String groupId, int commitBatchSize,
      int commitBatchTimeout) {
    return ConfigFactory.parseString(String.format(
        "kafka {\n"
            + "  bootstrapServers: \"%s\"\n"
            + "  consumerGroupId: \"%s\"\n"
            + "  maxPollIntervalSecs: 30\n"
            + "  pollIntervalMs: 100\n"
            + "  maxRequestSize: 10000000\n"
            + "}\n"
            + "worker {\n"
            + "  commitBatchSize: %d\n"
            + "  commitBatchTimeout: %d\n"
            + "}\n"
            + "pipelines: [{name: \"%s\", stages: [{class: \"com.kmwllc.lucille.stage.NopStage\"}]}]\n",
        embeddedKafka.getBrokersAsString(), groupId, commitBatchSize, commitBatchTimeout, pipelineName));
  }

  private static void produce(String topic, int count) throws Exception {
    embeddedKafka.addTopics(new NewTopic(topic, 1, (short) 1));
    Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
      for (int i = 0; i < count; i++) {
        String id = "doc" + i;
        producer.send(new ProducerRecord<>(topic, id, Document.create(id, "run1").toString())).get();
      }
    }
  }

  private static long waitForCommitted(Config config, String groupId, String topic, long expected)
      throws Exception {
    long deadline = System.currentTimeMillis() + 60000;
    long committed = -1;
    while (System.currentTimeMillis() < deadline) {
      committed = committedOffset(config, groupId, topic);
      if (committed >= expected) {
        return committed;
      }
      Thread.sleep(100);
    }
    return committed;
  }

  private static long committedOffset(Config config, String groupId, String topic) throws Exception {
    Properties adminProps = new Properties();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrapServers"));
    try (Admin admin = Admin.create(adminProps)) {
      Map<TopicPartition, OffsetAndMetadata> committed =
          admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
      OffsetAndMetadata offset = committed.get(new TopicPartition(topic, 0));
      return offset == null ? -1 : offset.offset();
    }
  }

  private static List<ConsumerRecord<String, String>> read(String topic, int expected) {
    Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(embeddedKafka, topic + "_inspector", false);
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    List<ConsumerRecord<String, String>> records = new ArrayList<>();
    try (Consumer<String, String> inspector = new DefaultKafkaConsumerFactory<String, String>(consumerProps)
        .createConsumer()) {
      TopicPartition tp = new TopicPartition(topic, 0);
      inspector.assign(List.of(tp));
      inspector.seekToBeginning(List.of(tp));
      long deadline = System.currentTimeMillis() + 60000;
      while (records.size() < expected && System.currentTimeMillis() < deadline) {
        inspector.poll(Duration.ofMillis(500)).forEach(records::add);
      }
    }
    return records;
  }
}
