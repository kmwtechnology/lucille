package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.KafkaDocument;
import com.typesafe.config.Config;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles a record whose key or value cannot be deserialized (a "poison" record). Without handling,
 * the consumer's position never advances past such a record, so every subsequent poll throws on it
 * and the partition is stuck.
 *
 * <p>Behavior is controlled by {@code kafka.onDeserializationError}:
 * <ul>
 *   <li>{@code fail} (default): log the record's topic, partition, offset and key at ERROR, send a FAIL
 *   event for it, and rethrow so the consumer stops. Restarting will hit the same record again.</li>
 *   <li>{@code skip}: log the same location at ERROR, send a FAIL event, and seek past the record. If
 *   {@code kafka.maxConsecutiveDeserializationErrors} (default 10) poison records arrive in a row on one
 *   partition, the cause is assumed to be systemic and the handler escalates to {@code fail}.</li>
 * </ul>
 *
 * <p>The record is never copied to the pipeline's fail topic, which holds only deserializable Documents.
 * It remains in the topic it was read from at the logged offset (subject to that topic's retention), so
 * recovery means re-reading that offset.
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
  private final Consumer<?, ?> consumer;
  private final MessageSender eventSender;
  private final boolean skip;
  private final int maxConsecutive;
  private final Map<TopicPartition, Integer> consecutiveFailures = new HashMap<>();

  private String lastRunId;

  /**
   * Sends an Event to the event topic. Lets each messenger keep using its own event producer.
   */
  interface MessageSender {
    void send(Event event) throws Exception;
  }

  DeserializationErrorHandler(Config config, Consumer<?, ?> consumer, MessageSender eventSender) {
    this.config = config;
    this.consumer = consumer;
    this.eventSender = eventSender;

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
    String location = "topic=" + tp.topic() + ", partition=" + tp.partition() + ", offset=" + e.offset()
        + ", key=" + key;
    String docId = key != null ? key : tp + "@" + e.offset();

    int consecutive = consecutiveFailures.merge(tp, 1, Integer::sum);
    boolean escalate = skip && consecutive >= maxConsecutive;

    if (!skip || escalate) {
      if (escalate) {
        log.error("Could not deserialize record ({}). This is the {}th consecutive undeserializable record on "
            + "this partition, which exceeds kafka.maxConsecutiveDeserializationErrors; stopping the consumer.",
            location, consecutive, e);
      } else {
        log.error("Could not deserialize record ({}); stopping the consumer. The record will be redelivered on "
            + "restart; set kafka.onDeserializationError: skip to skip it and continue.", location, e);
      }
      sendFailEvent(docId, "Could not deserialize record (" + location + ")");
      throw e;
    }

    log.error("Could not deserialize record ({}); skipping it. It remains in the topic at that offset, "
        + "subject to the topic's retention.", location, e);
    sendFailEvent(docId, "Could not deserialize record (" + location + "); skipped");
    consumer.seek(tp, e.offset() + 1);
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

  private static String bufferToString(ByteBuffer buffer) {
    if (buffer == null) {
      return null;
    }
    ByteBuffer copy = buffer.duplicate();
    byte[] bytes = new byte[copy.remaining()];
    copy.get(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }
}
