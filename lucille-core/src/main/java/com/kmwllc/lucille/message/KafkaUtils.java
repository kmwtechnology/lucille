package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.KafkaDocument;
import com.kmwllc.lucille.util.FileUtils;
import com.typesafe.config.Config;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Utilities for interacting with Kafka: creating Kafka clients, determining Kafka topic names, etc.
 * <p>
 * <p>
 *  TODO: add config option for cleaning up kafka topics at end of run
 *  prepend run id to each topic name
 *  add separate config option to leave failure topic around
 *  consider adding all failed documents to the failure topic
 *  add ability to override names of any kafka topic in the config
 */
public class KafkaUtils {

  public static final Duration POLL_INTERVAL = Duration.ofMillis(2000);

  private static Properties loadExternalProps(String filename) {
    try (Reader propertiesReader = FileUtils.getReader(filename, StandardCharsets.UTF_8.name())) {
      Properties consumerProps = new Properties();
      consumerProps.load(propertiesReader);
      return consumerProps;
    } catch (IOException e) {
      throw new IllegalStateException(String.format("Cannot load kafka property file %s.", filename));
    }
  }

  //access set to package so unit tests can validate created properties without initializing a Consumer
  static Properties createConsumerProps(Config config, String clientId) {
    if (config.hasPath("kafka.consumerPropertyFile")) {
      Properties loadedProps = loadExternalProps(config.getString("kafka.consumerPropertyFile"));
      loadedProps.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
      return loadedProps;
    }

    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrapServers"));
    if (config.hasPath("kafka.securityProtocol")) {
      consumerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, config.getString("kafka.securityProtocol"));
    }
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, config.getString("kafka.consumerGroupId"));
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1000 * config.getInt("kafka.maxPollIntervalSecs"));
    consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 30000);
    return consumerProps;
  }

  public static KafkaConsumer<String, KafkaDocument> createDocumentConsumer(Config config, String clientId) {
    Properties consumerProps = createConsumerProps(config, clientId);
    String deserializerClass = config.hasPath("kafka.documentDeserializer")
      ? config.getString("kafka.documentDeserializer")
      : KafkaDocumentDeserializer.class.getName();
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializerClass);
    return new KafkaConsumer<>(consumerProps);
  }

  public static KafkaConsumer<String, String> createEventConsumer(Config config, String clientId) {
    Properties consumerProps = createConsumerProps(config, clientId);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    return new KafkaConsumer<>(consumerProps);
  }

  //access set to package so unit tests can validate created properties without initializing a Producer
  static Properties createProducerProps(Config config) {
    if (config.hasPath("kafka.producerPropertyFile")) {
      return loadExternalProps(config.getString("kafka.producerPropertyFile"));
    }
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrapServers"));
    if (config.hasPath("kafka.securityProtocol")) {
      producerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, config.getString("kafka.securityProtocol"));
    }
    producerProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, config.getInt("kafka.maxRequestSize"));
    producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.getInt("kafka.maxRequestSize"));
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return producerProps;
  }

  public static KafkaProducer<String, Document> createDocumentProducer(Config config) {
    Properties producerProps = createProducerProps(config);
    String serializerClass = config.hasPath("kafka.documentSerializer")
      ? config.getString("kafka.documentSerializer")
      : KafkaDocumentSerializer.class.getName();
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializerClass);
    return new KafkaProducer<>(producerProps);
  }

  public static KafkaProducer<String, String> createEventProducer(Config config) {
    if (config.hasPath("kafka.events") && !config.getBoolean("kafka.events")) {
      return null;
    }
    Properties producerProps = createProducerProps(config);
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return new KafkaProducer<>(producerProps);
  }

  public static String getEventTopicName(String pipelineName, String runId) {
    return pipelineName + "_event_" + runId;
  }

  public static String getSourceTopicName(String pipelineName, Config config) {
    return config.hasPath("kafka.sourceTopic")
      ? config.getString("kafka.sourceTopic")
      : pipelineName + "_source";
  }

  public static String getDestTopicName(String pipelineName) {
    return pipelineName + "_dest";
  }

  public static String getFailTopicName(String pipelineName) {
    return pipelineName + "_fail";
  }

  public static void validateAtMostOneRecord(ConsumerRecords<?, ?> consumerRecords) throws Exception {
    if (consumerRecords.count() > 1) {
      throw new Exception("Kafka poll returned more than 1 message but this shouldn't happen");
    }
  }

  public static void createEventTopic(Config config, String pipelineName, String runId) throws ExecutionException, InterruptedException {
    String eventTopicName = KafkaUtils.getEventTopicName(pipelineName, runId);

    // create the event topic explicitly so we can guarantee that it will have 1 partition
    // multiple partitions could lead to events being received out of order which
    // could mean that child creation events arrive after parent completion events, which could
    // interfere with the publisher's logic for determining when the run is complete

    Properties props;
    if (config.hasPath("kafka.adminPropertyFile")) {
      props = loadExternalProps(config.getString("kafka.adminPropertyFile"));
    } else {
      props = new Properties();
      props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrapServers"));
    }

    try (Admin kafkaAdminClient = Admin.create(props)) {
      NewTopic eventTopic = new NewTopic(eventTopicName, 1, (short) 1);
      CreateTopicsResult result = kafkaAdminClient.createTopics(List.of(eventTopic), new CreateTopicsOptions());
      KafkaFuture<Void> future = result.values().get(eventTopicName);
      future.get();
    }
  }
}
