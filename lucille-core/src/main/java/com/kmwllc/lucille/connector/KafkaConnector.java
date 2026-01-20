package com.kmwllc.lucille.connector;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.message.KafkaUtils;
import com.typesafe.config.Config;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

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
      .requiredString("kafka.bootstrapServers", "kafka.topic", "kafka.consumerGroupId", "kafka.clientId", "kafka.maxPollIntervalSecs")
      .optionalString("idField")
      .optionalParent("offsets", new TypeReference<Map<Integer, Long>>() {})
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
  private final ObjectMapper mapper = new ObjectMapper();
  private KafkaConsumer<String, String> consumer;
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
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");


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

  private void startPollingLoop(Publisher publisher) throws ConnectorException {
    long count = 0;
    while (running && !Thread.currentThread().isInterrupted() && (maxMessages == null || count < maxMessages)) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(messageTimeout));

      if (records.isEmpty()) {
        if (!continueOnTimeout) {
          break;
        }
        continue;
      }

      for (ConsumerRecord<String, String> record : records) {
        handleMessage(record, publisher);
        count++;
        if (maxMessages != null && count >= maxMessages) {
          break;
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

  @Override
  public Spec getSpec() {
    return SPEC;
  }

  KafkaConsumer<String, String> createConsumer(Properties props) {
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
  public void handleMessage(ConsumerRecord<String, String> record, Publisher publisher) throws ConnectorException {
    Document doc = asDoc(record);
    try {
      publisher.publish(doc);
    } catch (Exception e) {
      throw new ConnectorException("Error publishing document", e);
    }
  }

  /**
   * Converts a Kafka consumer record into a Lucille document.
   * <p>
   * The record value is expected to be a JSON object. If an idField is configured and present in the
   * JSON object, its value will be used as the document ID.
   * <p>
   * Subclasses may choose to customize the creation of the lucille document by overriding this method.
   *
   * @param record the Kafka consumer record to convert
   * @return the created Lucille document
   * @throws ConnectorException if the record value is not a JSON object or if an error occurs during conversion
   */
  public Document asDoc(ConsumerRecord<String, String> record) throws ConnectorException {
    try {
      JsonNode node = mapper.readTree(record.value());
      if (!node.isObject()) {
        throw new ConnectorException("Consumer record value is not a JSON object: " + record.value());
      }
      ObjectNode objectNode = (ObjectNode) node;
      resolveDocumentId(objectNode);
      return Document.create(objectNode);
    } catch (Exception e) {
      throw new ConnectorException("Error converting consumer record to document", e);
    }
  }

  private void resolveDocumentId(ObjectNode objectNode) {
    if (idField != null && objectNode.has(idField)) {
      objectNode.put(Document.ID_FIELD, createDocId(objectNode.get(idField).asText()));
    } else if (objectNode.has(Document.ID_FIELD)) {
      objectNode.put(Document.ID_FIELD, createDocId(objectNode.get(Document.ID_FIELD).asText()));
    }
  }
}
