package com.kmwllc.lucille.connector;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.message.KafkaDocumentDeserializer;
import com.kmwllc.lucille.message.KafkaUtils;
import com.typesafe.config.Config;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connector for reading messages from a Kafka topic.
 *
 * Config Parameters -
 *
 * <ul>
 *   <li>bootstrapServers (String, Required) : Kafka bootstrap servers.</li>
 *   <li>topic (String, Required) : Kafka topic to read from.</li>
 *   <li>consumerGroupId (String, Required) : Kafka consumer group ID.</li>
 *   <li>idField (String, Optional) : The field in the Kafka message to use as the Lucille document ID.</li>
 *   <li>offsets (Map &lt;Integer, Long&gt;, Optional) : A map of partition numbers to initial offsets.</li>
 *   <li>maxMessages (Long, Optional) : The maximum number of messages to process before stopping.</li>
 *   <li>messageTimeout (Long, Optional) : The timeout in milliseconds to use when polling Kafka. Defaults to 100ms.</li>
 *   <li>continueOnTimeout (Boolean, Optional) : Whether to continue polling after a timeout. Defaults to true.</li>
 * </ul>
 */
public class KafkaConnector extends AbstractConnector {

  private static final Logger log = LoggerFactory.getLogger(KafkaConnector.class);

  public static final Spec SPEC = SpecBuilder.connector()
      .requiredString("kafka.bootstrapServers", "kafka.topic", "kafka.consumerGroupId", "kafka.clientId",
          "kafka.maxPollIntervalSecs")
      .optionalString("idField", "kafka.documentDeserializer")
      .optionalParent("offsets", new TypeReference<Map<Integer, Long>>() {
      })
      .optionalNumber("maxMessages", "messageTimeout")
      .optionalBoolean("continueOnTimeout")
      .build();

  private final String topic;
  private final String clientId;
  private final String idField;
  private final Map<Integer, Long> offsets;
  private final Long maxMessages;
  private final Long messageTimeout;
  private final boolean continueOnTimeout;
  private KafkaConsumer<String, Document> consumer;

  // running is volatile to ensure visibility to the polling thread.
  // It is used to ensure that the polling thread exits when the connector is closed.
  private volatile boolean running = true;

  public KafkaConnector(Config config) {
    super(config);
    this.topic = config.getString("kafka.topic");
    this.clientId = config.getString("kafka.clientId");
    this.idField = ConfigUtils.getOrDefault(config, "idField", null);
    this.maxMessages = ConfigUtils.getOrDefault(config, "maxMessages", null);
    this.messageTimeout = ConfigUtils.getOrDefault(config, "messageTimeout", 100L);
    this.continueOnTimeout = ConfigUtils.getOrDefault(config, "continueOnTimeout", true);
    this.offsets = parseOffsets(config);
  }

  private Map<Integer, Long> parseOffsets(Config config) {
    if (!config.hasPath("offsets")) {
      return null;
    }
    return config.getConfig("offsets").entrySet().stream()
        .collect(Collectors.toMap(
            e -> Integer.parseInt(e.getKey()),
            e -> Long.parseLong(e.getValue().unwrapped().toString())));
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {

    Properties props = KafkaUtils.createConsumerProps(config, clientId);
    // The Kafka connector implementation will poll for new messages after the current batch is fully processed either by

    // custom handling or publishing with the publisher.
    enhanceConsumerProperties(props, config);

    consumer = createConsumer(props);

    try {
      consumer.subscribe(Collections.singletonList(topic));

      if (offsets != null) {
        log.info("Seeking to specified offsets: {}", offsets);
        consumer.poll(Duration.ZERO);
        for (TopicPartition partition : consumer.assignment()) {
          if (offsets.containsKey(partition.partition())) {
            Long offset = offsets.get(partition.partition());
            log.info("Seeking partition {} to offset {}", partition.partition(), offset);
            consumer.seek(partition, offset);
          }
        }
      }

      startPollingLoop(publisher);
    } catch (Exception e) {
      throw new ConnectorException("Error reading from Kafka", e);
    }
  }

  /**
   * Updates the consumer properties with any additional properties required by the Kafka connector.
   * Updates the consumer properties with a custom deserializer for the document value if configured.
   * Updates the consumer properties with the configured idField if present in the configuration.
   * Updates the consumer properties with the docIdPrefix configured for this connector.
   *
   * If a custom deserializer is used, this method can be overridden to add additional properties needed by the custom deserializer.
   * @param props - Kafka consumer properties
   * @param config - Lucille connector configuration
   */
  public void enhanceConsumerProperties(Properties props, Config config) {
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    String deserializerClass = config.hasPath("kafka.documentDeserializer")
        ? config.getString("kafka.documentDeserializer")
        : KafkaConnectorDefaultDeserializer.class.getName();
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializerClass);
    if (idField != null) {
      props.put("idField", idField);
    }
    props.put("docIdPrefix", getDocIdPrefix());
  }

  private void startPollingLoop(Publisher publisher) throws ConnectorException {
    long count = 0;
    while (running && !Thread.currentThread().isInterrupted() && (maxMessages == null || count < maxMessages)) {
      ConsumerRecords<String, Document> records = consumer.poll(Duration.ofMillis(messageTimeout));

      if (records.isEmpty()) {
        if (continueOnTimeout) {
          continue;
        } else {
          return;
        }
      }

      for (ConsumerRecord<String, Document> record : records) {
        handleMessage(record, publisher);
        count++;
        if (maxMessages != null && count >= maxMessages) {
          return;
        }
      }
    }
  }

  @Override
  public void close() throws ConnectorException {
    log.info("Closing KafkaConnector and consumer");
    running = false;

    if (consumer != null) {
      consumer.wakeup();
      consumer.close();
      consumer = null;
    }

  }

  // access set to package so unit tests provide a mock consumer by overriding this method.
  KafkaConsumer<String, Document> createConsumer(Properties props) {
    return new KafkaConsumer<>(props);
  }

  /**
   * Processes a single consumer record by converting it to a Lucille document and publishing it.
   * <p>
   * Subclasses may choose to handle messages in a different way and may or may not publish the
   * document depending on the message and the implementation of the subclassed KafkaConnector.
   *
   * @param record the Kafka consumer record to process
   * @param publisher the publisher to use for publishing the document
   * @throws ConnectorException if an error occurs during conversion or publishing
   */
  public void handleMessage(ConsumerRecord<String, Document> record, Publisher publisher) throws ConnectorException {
    Document doc = record.value();
    try {
      publisher.publish(doc);
    } catch (Exception e) {
      throw new ConnectorException("Error publishing document", e);
    }
  }
}
