package com.kmwllc.lucille.message;

import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Properties;

/**
 * Utilities for interacting with Kafka: creating Kafka clients, determining Kafka topic names, etc.
 */
public class KafkaUtils {

  public static final Duration POLL_INTERVAL = Duration.ofMillis(2000);

  public static Properties createConsumerProps(Config config) {
    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrapServers"));
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, config.getString("kafka.consumerGroupId"));
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1000 * config.getInt("kafka.maxPollIntervalSecs"));
    consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,  StringDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    return consumerProps;
  }

  public static KafkaProducer<String,String> createProducer(Config config) {
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrapServers"));
    producerProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, config.getInt("kafka.maxRequestSize"));
    producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.getInt("kafka.maxRequestSize"));
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return new KafkaProducer(producerProps);
  }

  public static String getEventTopicName(String pipelineName, String runId) {
    return pipelineName + "_event_" + runId;
  }

  public static String getSourceTopicName(String pipelineName) {
   return pipelineName + "_source";
  }

  public static String getDestTopicName(String pipelineName) {
    return pipelineName + "_dest";
  }

  public static String getFailTopicName(String pipelineName) {
    return pipelineName + "_fail";
  }

  public static void validateAtMostOneRecord(ConsumerRecords<String, String> consumerRecords) throws Exception {
    if (consumerRecords.count() > 1) {
      throw new Exception("Kafka poll returned more than 1 message but this shouldn't happen");
    }
  }
}
