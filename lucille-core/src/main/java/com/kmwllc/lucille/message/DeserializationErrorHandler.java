package com.kmwllc.lucille.message;

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
 *   <li>{@code fail} (default): log the record's topic, partition, offset and key at ERROR and rethrow so the
 *   consumer stops. Restarting will hit the same record again.</li>
 *   <li>{@code skip}: log the same location at ERROR and seek past the record. If
 *   {@code kafka.maxConsecutiveDeserializationErrors} (default 10) poison records arrive in a row on one
 *   partition, the cause is assumed to be systemic and the handler escalates to {@code fail}. A value of
 *   {@code -1} means unlimited: skip mode never escalates. Any other value below 1 is rejected.</li>
 * </ul>
 *
 * <p>The record is never copied to the pipeline's fail topic, which holds only deserializable Documents.
 * It remains in the topic it was read from at the logged offset (subject to that topic's retention), so
 * recovery means re-reading that offset. The ERROR log is the only signal.
 *
 * <p>No FAIL event is sent. A record that cannot be parsed carries no run id, and a long-lived consumer may
 * serve several runs at once, so any run id chosen for the event could belong to an uninvolved run and corrupt
 * its accounting. The tradeoff: a skipped poison record produces no terminal event for its run, so in a batch
 * run that run's pending count never reaches zero and the run waits out its timeout.
 */
class DeserializationErrorHandler {

  private static final Logger log = LoggerFactory.getLogger(DeserializationErrorHandler.class);

  static final String MODE_FAIL = "fail";
  static final String MODE_SKIP = "skip";
  static final int DEFAULT_MAX_CONSECUTIVE = 10;
  static final int UNLIMITED = -1;

  private final Consumer<?, ?> consumer;
  private final boolean skip;
  private final int maxConsecutive;
  private final Map<TopicPartition, Integer> consecutiveFailures = new HashMap<>();

  DeserializationErrorHandler(Config config, Consumer<?, ?> consumer) {
    this.consumer = consumer;

    String mode = config.hasPath("kafka.onDeserializationError")
        ? config.getString("kafka.onDeserializationError") : MODE_FAIL;
    if (!MODE_FAIL.equals(mode) && !MODE_SKIP.equals(mode)) {
      throw new IllegalArgumentException("kafka.onDeserializationError must be \"" + MODE_FAIL + "\" or \""
          + MODE_SKIP + "\" but was \"" + mode + "\"");
    }
    this.skip = MODE_SKIP.equals(mode);

    this.maxConsecutive = config.hasPath("kafka.maxConsecutiveDeserializationErrors")
        ? config.getInt("kafka.maxConsecutiveDeserializationErrors") : DEFAULT_MAX_CONSECUTIVE;
    if (maxConsecutive < 1 && maxConsecutive != UNLIMITED) {
      throw new IllegalArgumentException("kafka.maxConsecutiveDeserializationErrors must be at least 1, or "
          + UNLIMITED + " for unlimited, but was " + maxConsecutive);
    }
  }

  /**
   * Called for each record that was polled and deserialized successfully; resets its partition's count of consecutive
   * undeserializable records.
   */
  void onSuccessfulPoll(ConsumerRecord<?, ?> record) {
    consecutiveFailures.remove(new TopicPartition(record.topic(), record.partition()));
  }

  /**
   * Handles a {@link RecordDeserializationException} from a poison record, either by skipping the record or by
   * rethrowing the exception.
   *
   * <p>In skip mode, logs the record's location, positions the consumer after it and returns normally. Rethrows
   * {@code e} in fail mode, and also in skip mode once the partition reaches
   * {@code kafka.maxConsecutiveDeserializationErrors} consecutive undeserializable records.
   */
  void handleOrRethrow(RecordDeserializationException e) throws RecordDeserializationException {
    TopicPartition tp = e.topicPartition();
    String location = "topic=" + tp.topic() + ", partition=" + tp.partition() + ", offset=" + e.offset()
        + ", key=" + bufferToString(e.keyBuffer());

    int consecutive = consecutiveFailures.merge(tp, 1, Integer::sum);
    boolean escalate = skip && maxConsecutive != UNLIMITED && consecutive >= maxConsecutive;

    if (!skip) {
      log.error("Could not deserialize record ({}); stopping the consumer. The record will be redelivered on "
          + "restart; set kafka.onDeserializationError: skip to skip it and continue.", location, e);
      throw e;
    }
    if (escalate) {
      log.error("Could not deserialize record ({}). This is the {}th consecutive undeserializable record on "
          + "this partition, which reaches kafka.maxConsecutiveDeserializationErrors; stopping the consumer.",
          location, consecutive, e);
      throw e;
    }

    log.error("Could not deserialize record ({}); skipping it. It remains in the topic at that offset, "
        + "subject to the topic's retention.", location, e);
    consumer.seek(tp, e.offset() + 1);
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
