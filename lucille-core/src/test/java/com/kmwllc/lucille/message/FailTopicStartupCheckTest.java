package com.kmwllc.lucille.message;

import static com.kmwllc.lucille.util.ThreadNameUtils.areLucilleThreadsRunning;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.WorkerIndexerPool;
import com.kmwllc.lucille.util.CounterUtils;
import com.kmwllc.lucille.util.EmbeddedKafkaUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

/**
 * Tests that a Kafka messenger that may send to the fail topic checks for it as it is constructed, against a broker
 * that does not create topics automatically, where a missing fail topic would otherwise only be noticed when the first
 * document fails.
 */
public class FailTopicStartupCheckTest {

  private static final AtomicInteger counter = new AtomicInteger();

  private static EmbeddedKafkaBroker embeddedKafka;
  private static Admin admin;

  @BeforeClass
  public static void startKafka() {
    embeddedKafka = EmbeddedKafkaUtils.startBroker(Map.of("auto.create.topics.enable", "false"));
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

  // A pipeline name no other test in this JVM uses, since a fail topic found to exist is not checked again.
  private static String newPipeline() {
    return "failTopicCheck" + counter.incrementAndGet();
  }

  private static Config config(String pipeline, Map<String, Object> extra) {
    Map<String, Object> settings = new HashMap<>(Map.of(
        "kafka.bootstrapServers", embeddedKafka.getBrokersAsString(),
        "kafka.consumerGroupId", "group-" + pipeline,
        "kafka.maxPollIntervalMs", 300000,
        "kafka.maxRequestSize", 1048576,
        "kafka.events", false,
        // how long the producer waits for the metadata of a topic that does not exist: 60 seconds by default
        "kafka.producer.\"max.block.ms\"", 2000));
    settings.putAll(extra);
    return ConfigFactory.parseMap(settings);
  }

  private static void assertNames(IllegalStateException e, String topic, String setting) {
    assertTrue(e.getMessage(), e.getMessage().contains("\"" + topic + "\""));
    assertTrue(e.getMessage(), e.getMessage().contains(setting));
    assertTrue(e.getMessage(), e.getMessage().contains("kafka.failTopic"));
  }

  @Test
  public void testWorkerMessengersDoNotStartWithoutTheFailTopic() {
    String pipeline = newPipeline();
    Config config = config(pipeline, Map.of("worker.maxRetries", 2));

    assertNames(assertThrows(IllegalStateException.class, () -> new KafkaWorkerMessenger(config, pipeline)),
        pipeline + "_fail", "worker.maxRetries");
    assertNames(assertThrows(IllegalStateException.class,
        () -> new HybridWorkerMessenger(config, pipeline, new LinkedBlockingQueue<>())), pipeline + "_fail", "worker.maxRetries");
  }

  @Test
  public void testIndexerMessengersDoNotStartWithoutTheFailTopic() {
    String pipeline = newPipeline();
    Config config = config(pipeline, Map.of("indexer.sendFailedDocsToTopic", true));

    assertNames(assertThrows(IllegalStateException.class, () -> new KafkaIndexerMessenger(config, pipeline)),
        pipeline + "_fail", "indexer.sendFailedDocsToTopic");
    assertNames(assertThrows(IllegalStateException.class,
            () -> new HybridIndexerMessenger(config, new LinkedBlockingQueue<>(), null, null, pipeline)),
        pipeline + "_fail", "indexer.sendFailedDocsToTopic");
  }

  @Test
  public void testTheConfiguredFailTopicIsTheOneChecked() {
    String pipeline = newPipeline();
    String failTopic = "dead-letter." + pipeline;
    // the default name exists, but it is not the one in use
    embeddedKafka.addTopics(new NewTopic(pipeline + "_fail", 1, (short) 1));
    Config config = config(pipeline, Map.of("worker.sendFailedDocsToTopic", true, "kafka.failTopic", failTopic));

    assertNames(assertThrows(IllegalStateException.class, () -> new KafkaWorkerMessenger(config, pipeline)),
        failTopic, "worker.sendFailedDocsToTopic");
  }

  @Test
  public void testNothingIsCheckedWhenTheFailTopicIsNotEnabled() throws Exception {
    String pipeline = newPipeline();
    Config config = config(pipeline, Map.of());

    new KafkaWorkerMessenger(config, pipeline).close();
    new HybridWorkerMessenger(config, pipeline, new LinkedBlockingQueue<>()).close();
    new KafkaIndexerMessenger(config, pipeline).close();
    new HybridIndexerMessenger(config, new LinkedBlockingQueue<>(), null, null, pipeline).close();

    // a Worker's setting does not make an Indexer check, and the other way round
    new KafkaIndexerMessenger(config(pipeline, Map.of("worker.maxRetries", 2)), pipeline).close();
    new KafkaWorkerMessenger(config(pipeline, Map.of("indexer.sendFailedDocsToTopic", true)), pipeline).close();
  }

  @Test
  public void testMessengersStartOnceTheFailTopicExists() throws Exception {
    String pipeline = newPipeline();
    Config workerConfig = config(pipeline, Map.of("worker.maxRetries", 2));
    Config indexerConfig = config(pipeline, Map.of("indexer.sendFailedDocsToTopic", true));
    assertThrows(IllegalStateException.class, () -> new KafkaWorkerMessenger(workerConfig, pipeline));

    embeddedKafka.addTopics(new NewTopic(pipeline + "_fail", 1, (short) 1));

    new KafkaWorkerMessenger(workerConfig, pipeline).close();
    new HybridWorkerMessenger(workerConfig, pipeline, new LinkedBlockingQueue<>()).close();
    new KafkaIndexerMessenger(indexerConfig, pipeline).close();
    new HybridIndexerMessenger(indexerConfig, new LinkedBlockingQueue<>(), null, null, pipeline).close();
  }

  @Test
  public void testTheCheckDoesNotCreateTheTopic() throws Exception {
    String pipeline = newPipeline();
    Config config = config(pipeline, Map.of("indexer.sendFailedDocsToTopic", true));
    assertThrows(IllegalStateException.class, () -> new KafkaIndexerMessenger(config, pipeline));
    assertFalse(admin.listTopics().names().get().contains(pipeline + "_fail"));
  }

  @Test
  public void testWorkerIndexerPoolDoesNotStartWithoutTheFailTopic() throws Exception {
    String pipeline = newPipeline();
    Config config = config(pipeline, Map.of("worker.sendFailedDocsToTopic", true, "worker.threads", 2))
        .withFallback(ConfigFactory.parseString(
            "pipelines: [{name: \"" + pipeline + "\", stages: []}], indexer { type: \"Nop\", sendEnabled: false }"));
    WorkerIndexerPool pool = new WorkerIndexerPool(config, pipeline, true, CounterUtils.getThreadSafeSet());

    assertNames(assertThrows(IllegalStateException.class, pool::start), pipeline + "_fail", "worker.sendFailedDocsToTopic");
    assertFalse(pool.isAlive());
    assertFalse(areLucilleThreadsRunning());
  }
}
