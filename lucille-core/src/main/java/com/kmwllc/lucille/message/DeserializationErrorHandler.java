package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.KafkaDocument;
import com.typesafe.config.Config;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles a record whose key or value cannot be deserialized (a "poison" record). Without handling,
 * the consumer's position never advances past such a record, so every subsequent poll throws on it
 * and the partition is stuck.
 *
 * <p>Behavior is controlled by {@code kafka.onDeserializationError}:
 * <ul>
 *   <li>{@code fail} (default): log the record's topic/partition/offset/key at ERROR, send a FAIL
 *   event for it, and rethrow so the consumer stops. Restarting will hit the same record again.</li>
 *   <li>{@code skip}: log at ERROR, copy the raw bytes to the pipeline's fail topic (when a dead letter
 *   topic is enabled), send a FAIL event, and seek past the record. If
 *   {@code kafka.maxConsecutiveDeserializationErrors} (default 10) poison records arrive in a row on one
 *   partition, the cause is assumed to be systemic and the handler escalates to {@code fail}.</li>
 * </ul>
 *
 * <p>The FAIL event is keyed by the record key (the document id). A record that cannot be parsed carries
 * no run id, so the event is sent with the run id of the last document successfully polled by this
 * consumer; if there is none and no fixed {@code kafka.eventTopic} is configured, no event is sent.
 */
class DeserializationErrorHandler {

  private static final Logger log = LoggerFactory.getLogger(DeserializationErrorHandler.class);

  static final String MODE_FAIL = "fail";
  static final String MODE_SKIP = "skip";
  static final int DEFAULT_MAX_CONSECUTIVE = 10;

  private final Config config;
  private final String pipelineName;
  private final Consumer<?, ?> consumer;
  private final MessageSender eventSender;
  private final boolean deadLetter;
  private final boolean skip;
  private final int maxConsecutive;
  private final Map<TopicPartition, Integer> consecutiveFailures = new HashMap<>();

  private KafkaProducer<String, byte[]> deadLetterProducer;
  private String lastRunId;

  /**
   * Sends an Event to the event topic. Lets each messenger keep using its own event producer.
   */
  interface MessageSender {
    void send(Event event) throws Exception;
  }

  /**
   * @param deadLetter whether skipped records should be copied to the pipeline's fail topic
   */
  DeserializationErrorHandler(Config config, String pipelineName, Consumer<?, ?> consumer,
      MessageSender eventSender, boolean deadLetter) {
    this.config = config;
    this.pipelineName = pipelineName;
    this.consumer = consumer;
    this.eventSender = eventSender;
    this.deadLetter = deadLetter;

    String mode = config.hasPath("kafka.onDeserializationError")
        ? config.getString("kafka.onDeserializationError") : MODE_FAIL;
    if (!MODE_FAIL.equals(mode) && !MODE_SKIP.equals(mode)) {
      throw new IllegalArgumentException("kafka.onDeserializationError must be \"" + MODE_FAIL + "\" or \""
          + MODE_SKIP + "\" but was \"" + mode + "\"");
    }
    this.skip = MODE_SKIP.equals(mode);
    this.maxConsecutive = config.hasPath("kafka.maxConsecutiveDeserializationErrors")
        ? config.getInt("kafka.maxConsecutiveDeserializationErrors") : DEFAULT_MAX_CONSECUTIVE;
  }

  /**
   * Records that a document was successfully deserialized, resetting the consecutive-failure count for its
   * partition and remembering its run id for later FAIL events.
   */
  void recordSuccess(ConsumerRecord<String, KafkaDocument> record) {
    consecutiveFailures.remove(new TopicPartition(record.topic(), record.partition()));
    if (record.value() != null && record.value().getRunId() != null) {
      lastRunId = record.value().getRunId();
    }
  }

  /**
   * Handles a poison record. Returns normally if the record was skipped and the consumer has been positioned
   * after it; otherwise rethrows {@code e}.
   */
  void handle(RecordDeserializationException e) throws RecordDeserializationException {
    TopicPartition tp = e.topicPartition();
    String key = bufferToString(e.keyBuffer());
    String location = tp + "@" + e.offset();
    String docId = key != null ? key : location;

    int consecutive = consecutiveFailures.merge(tp, 1, Integer::sum);
    boolean escalate = skip && consecutive >= maxConsecutive;

    if (!skip || escalate) {
      if (escalate) {
        log.error("Could not deserialize record {} (key {}). This is the {}th consecutive undeserializable record on "
            + "this partition, which exceeds kafka.maxConsecutiveDeserializationErrors; stopping the consumer.",
            location, key, consecutive, e);
      } else {
        log.error("Could not deserialize record {} (key {}); stopping the consumer. The record will be redelivered "
            + "on restart; set kafka.onDeserializationError: skip to dead-letter it and continue.", location, key, e);
      }
      sendFailEvent(docId, "Could not deserialize record " + location);
      throw e;
    }

    log.error("Could not deserialize record {} (key {}); skipping it.", location, key, e);
    if (deadLetter) {
      sendToDeadLetter(e, key);
    }
    sendFailEvent(docId, "Could not deserialize record " + location + "; skipped");
    consumer.seek(tp, e.offset() + 1);
  }

  private void sendToDeadLetter(RecordDeserializationException e, String key) {
    String failTopic = KafkaUtils.getFailTopicName(pipelineName);
    try {
      if (deadLetterProducer == null) {
        Properties props = KafkaUtils.createProducerProps(config);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        deadLetterProducer = new KafkaProducer<>(props);
      }
      ProducerRecord<String, byte[]> record =
          new ProducerRecord<>(failTopic, null, key, bufferToBytes(e.valueBuffer()), e.headers());
      deadLetterProducer.send(record).get();
    } catch (Exception ex) {
      log.error("Failed to send undeserializable record {}@{} to {}", e.topicPartition(), e.offset(), failTopic, ex);
    }
  }

  private void sendFailEvent(String docId, String message) {
    if (lastRunId == null && !config.hasPath("kafka.eventTopic")) {
      log.warn("No FAIL event sent for undeserializable record {}: its run id is unknown.", docId);
      return;
    }
    try {
      eventSender.send(new Event(docId, lastRunId, message, Event.Type.FAIL));
    } catch (Exception ex) {
      log.error("Failed to send FAIL event for undeserializable record {}", docId, ex);
    }
  }

  void close() {
    if (deadLetterProducer != null) {
      deadLetterProducer.close();
    }
  }

  private static byte[] bufferToBytes(ByteBuffer buffer) {
    if (buffer == null) {
      return null;
    }
    ByteBuffer copy = buffer.duplicate();
    byte[] bytes = new byte[copy.remaining()];
    copy.get(bytes);
    return bytes;
  }

  private static String bufferToString(ByteBuffer buffer) {
    byte[] bytes = bufferToBytes(buffer);
    return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
  }
}
