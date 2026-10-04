package com.kmwllc.lucille.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.KafkaDocument;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

/**
 * Tests how KafkaIndexerMessenger commits destination-topic offsets. Offsets are committed when a batch completes
 * (at-least-once) rather than when a document is polled, so a crash before completion re-delivers the document rather
 * than losing it. Uses a MockConsumer, which tracks committed offsets, to assert the commit behavior directly.
 */
public class KafkaIndexerMessengerCommitTest {

  private static final String TOPIC = KafkaUtils.getDestTopicName("pipeline1");
  private static final TopicPartition PARTITION_0 = new TopicPartition(TOPIC, 0);

  @Test
  public void testOffsetsCommittedOnBatchCompletionNotAtPoll() throws Exception {
    MockConsumer<String, KafkaDocument> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    // The real document consumer is configured with max.poll.records = 1; MockConsumer needs this set explicitly so
    // each poll returns a single record, matching production (and KafkaUtils.validateAtMostOneRecord).
    consumer.setMaxPollRecords(1);
    KafkaIndexerMessenger messenger = new KafkaIndexerMessenger(config(), "pipeline1", consumer);
    consumer.rebalance(Set.of(PARTITION_0));
    consumer.updateBeginningOffsets(Map.of(PARTITION_0, 0L));
    consumer.addRecord(record(0));
    consumer.addRecord(record(1));

    Document first = messenger.pollDocToIndex();
    Document second = messenger.pollDocToIndex();
    // Nothing is committed merely by polling: a crash here must re-deliver both documents, not skip them.
    assertNull(committedOffset(consumer));

    // Completing the first batch commits the offset after its last record (0 -> next offset 1).
    messenger.batchComplete(java.util.List.of(first));
    assertEquals(Long.valueOf(1L), committedOffset(consumer));

    // Completing the second batch advances the commit to 2.
    messenger.batchComplete(java.util.List.of(second));
    assertEquals(Long.valueOf(2L), committedOffset(consumer));
  }

  private static Config config() {
    return ConfigFactory.parseMap(Map.of(
        "kafka.bootstrapServers", "localhost:9092",
        "kafka.consumerGroupId", "test-group",
        "kafka.maxPollIntervalSecs", 300,
        "kafka.maxRequestSize", 1048576,
        "kafka.events", false));
  }

  // A destination-topic record whose value is a KafkaDocument carrying its own Kafka metadata.
  private static ConsumerRecord<String, KafkaDocument> record(long offset) {
    String id = "doc" + offset;
    try {
      KafkaDocument doc = new KafkaDocument(
          new ConsumerRecord<>(TOPIC, 0, offset, id, Document.create(id, "run1").toString()));
      return new ConsumerRecord<>(TOPIC, 0, offset, id, doc);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // The committed offset for partition 0, or null if nothing has been committed.
  private static Long committedOffset(MockConsumer<String, KafkaDocument> consumer) {
    OffsetAndMetadata committed = consumer.committed(Set.of(PARTITION_0)).get(PARTITION_0);
    return committed == null ? null : committed.offset();
  }
}
