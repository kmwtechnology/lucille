package com.kmwllc.lucille.util;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * Starts the embedded Kafka broker that tests share, configured so that no test pays the broker's one-time group
 * coordinator start-up cost inside its own time budget.
 *
 * <p>A KRaft broker creates its internal <code>__consumer_offsets</code> topic lazily, when the first consumer group
 * joins. Until the topic exists and its coordinator partition is loaded, every JoinGroup is answered with
 * "not the correct coordinator" and retried. With the default 50 partitions that takes a second or two on an idle
 * machine and nine seconds or more after a full test run, which is how HybridKafkaTest's pool test (the first test in
 * its class) came to time out only under load. Two measures remove the problem: the topic gets one partition, and a
 * throwaway consumer joins a group here, before any test starts, so the creation happens now rather than later.
 *
 * <p>The broker's initial rebalance delay (3 seconds by default, during which a new group waits for more members
 * before its first assignment) is also set to zero, so every group a test starts is assigned its partitions
 * immediately.
 */
public final class EmbeddedKafkaUtils {

  private static final Logger log = LoggerFactory.getLogger(EmbeddedKafkaUtils.class);

  private static final String WARM_UP_TOPIC = "coordinator_warm_up";
  private static final Duration WARM_UP_TIMEOUT = Duration.ofSeconds(60);

  private EmbeddedKafkaUtils() {
  }

  /**
   * Starts a single-node embedded KRaft broker whose group coordinator is ready for use. The caller destroys it.
   */
  public static EmbeddedKafkaBroker startBroker() {
    EmbeddedKafkaKraftBroker broker = new EmbeddedKafkaKraftBroker(1, 1);
    broker.brokerProperties(Map.of("group.initial.rebalance.delay.ms", "0", "offsets.topic.num.partitions", "1"));
    broker.afterPropertiesSet();
    warmUpGroupCoordinator(broker);
    return broker;
  }

  /**
   * Joins a throwaway consumer group and waits for its partition assignment, which forces the broker to create and
   * load its <code>__consumer_offsets</code> topic. Fails if the coordinator is not ready within a minute.
   */
  public static void warmUpGroupCoordinator(EmbeddedKafkaBroker broker) {
    broker.addTopics(new NewTopic(WARM_UP_TOPIC, 1, (short) 1));

    Map<String, Object> props = KafkaTestUtils.consumerProps(broker, "coordinator_warm_up_group", false);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    long start = System.currentTimeMillis();
    try (Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<String, String>(props).createConsumer()) {
      consumer.subscribe(List.of(WARM_UP_TOPIC));
      while (consumer.assignment().isEmpty()) {
        consumer.poll(Duration.ofMillis(200));
        if (System.currentTimeMillis() - start > WARM_UP_TIMEOUT.toMillis()) {
          throw new IllegalStateException("Embedded Kafka group coordinator not ready after " + WARM_UP_TIMEOUT);
        }
      }
    }
    log.info("Embedded Kafka group coordinator ready after {} ms", System.currentTimeMillis() - start);
  }
}
