package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.KafkaDocument;
import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * IndexerMessenger that consumes Documents from a pipeline's destination topic and sends Events to Kafka.
 *
 * <p> <b>Offset commits</b> depend on indexer.maxConcurrentBatches (see {@link Indexer}):
 * <ul>
 *   <li>1 (the default): each record's offset is committed as soon as it is polled, before it is indexed. A crash can
 *   lose about one batch.</li>
 *   <li>Greater than 1: the indexer keeps polling while batches are in flight, so committing at poll could lose all of
 *   them. Instead, {@link #batchComplete(List)} commits, per partition, the offset after the batch's last record. Delivery
 *   is then at-least-once: a crash or rebalance redelivers in-flight documents, which may be indexed twice and get
 *   duplicate completion events, but never loses them. (Except with indexer.indexOverrideField: batches are then kept per
 *   index, so a commit can pass a document still waiting in another index's partly filled batch.) When the topic is
 *   idle, a finished batch's completion can wait up to one poll interval ({@link KafkaUtils#POLL_INTERVAL}).
 *   enable.auto.commit must not be set to true.</li>
 * </ul>
 *
 * <p> In both cases, kafka.maxPollIntervalSecs must exceed the longest time the indexer can go without polling: one
 * batch's send with its retries or, for a batch containing a delete-by-query, the sends of the batches ahead of it plus
 * its own.
 */
public class KafkaIndexerMessenger implements IndexerMessenger {

  private static final Logger log = LoggerFactory.getLogger(KafkaIndexerMessenger.class);
  private final Consumer<String, KafkaDocument> destConsumer;
  // true when indexer.maxConcurrentBatches > 1: offsets are committed in batchComplete rather than at poll
  private final boolean commitOnBatchComplete;
  // offsets of completed batches not yet committed; only used when commitOnBatchComplete, on the indexer thread
  private final Map<TopicPartition, Long> pendingOffsets = new HashMap<>();
  private final KafkaProducer<String, String> kafkaEventProducer;
  private final String pipelineName;
  private final Config config;

  public KafkaIndexerMessenger(Config config, String pipelineName) {
    this(config, pipelineName, KafkaUtils.createDocumentConsumer(config, "com.kmwllc.lucille-indexer-" + pipelineName));
  }

  // for tests: allows a MockConsumer to be supplied
  KafkaIndexerMessenger(Config config, String pipelineName, Consumer<String, KafkaDocument> destConsumer) {
    this.pipelineName = pipelineName;
    this.destConsumer = destConsumer;
    this.commitOnBatchComplete = Indexer.getMaxConcurrentBatches(config) > 1;
    if (commitOnBatchComplete && isAutoCommitEnabled(config)) {
      destConsumer.close();
      throw new IllegalArgumentException("indexer.maxConcurrentBatches > 1 commits offsets once batches complete, so "
          + ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG + " must not be true for the indexer's Kafka consumer.");
    }
    this.destConsumer.subscribe(Collections.singletonList(KafkaUtils.getDestTopicName(pipelineName)));
    this.kafkaEventProducer = KafkaUtils.createEventProducer(config);
    this.config = config;
  }

  /**
   * Polls for a document that has been processed by the pipeine and is waiting to be indexed.
   */
  @Override
  public Document pollDocToIndex() throws Exception {
    ConsumerRecords<String, KafkaDocument> consumerRecords = destConsumer.poll(KafkaUtils.POLL_INTERVAL);
    KafkaUtils.validateAtMostOneRecord(consumerRecords);
    if (consumerRecords.count() > 0) {
      if (!commitOnBatchComplete) {
        // offsets are committed synchronously to ensure that offsets are successfully committed and to reduce the likelihood of duplicate events being sent to the event topic.
        // This reduces the number of documents that might be reindexed in the event of an indexer crash/restart or in the case of a consumer group reblance.
        destConsumer.commitSync();
      }
      ConsumerRecord<String, KafkaDocument> record = consumerRecords.iterator().next();
      KafkaDocument doc = record.value();
      doc.setKafkaMetadata(record);
      return doc;
    }
    return null;
  }

  @Override
  public void sendEvent(Document document, String message, Event.Type type) throws Exception {
    if (kafkaEventProducer == null) {
      return;
    }
    Event event = new Event(document, message, type);
    sendEvent(event);
  }


  /**
   * Sends an Event relating to a Document to the appropriate location for Events.
   *
   */
  @Override
  public void sendEvent(Event event) throws Exception {
    if (kafkaEventProducer == null) {
      return;
    }
    String confirmationTopicName = KafkaUtils.getEventTopicName(config, pipelineName, event.getRunId());
    RecordMetadata result = (RecordMetadata) kafkaEventProducer.send(
        new ProducerRecord(confirmationTopicName, event.getDocumentId(), event.toString())).get();
  }

  @Override
  public void sendEvents(List<Document> documents, String message, Event.Type type) throws Exception {
    if (kafkaEventProducer == null || documents.isEmpty()) {
      return;
    }

    AtomicReference<Exception> sendException = new AtomicReference<>();

    for (Document document : documents) {
      Event event = new Event(document, message, type);
      String confirmationTopicName = KafkaUtils.getEventTopicName(config, pipelineName, event.getRunId());
      kafkaEventProducer.send(
          new ProducerRecord<>(confirmationTopicName, event.getDocumentId(), event.toString()),
          (metadata, exception) -> {
            if (exception != null) {
              log.error("Failed to send event for doc: {}", event.getDocumentId(), exception);
              sendException.compareAndSet(null, exception);
            }
          });
    }

    kafkaEventProducer.flush();

    Exception e = sendException.get();
    if (e != null) {
      throw new Exception("Failed to send one or more events", e);
    }
  }

  @Override
  public void close() throws Exception {
    if (!pendingOffsets.isEmpty()) {
      try {
        commitPending();
      } catch (Exception e) {
        log.warn("Could not commit offsets {} on close; those documents will be delivered again.", pendingOffsets, e);
      }
    }
    destConsumer.close();
  }

  /**
   * When indexer.maxConcurrentBatches is greater than 1, commits, per partition, the offset after the batch's last record,
   * along with any offsets left by an earlier failed commit. Otherwise does nothing, since offsets were committed at poll.
   *
   * <p> Committing offset N is safe because the Indexer completes batches in the order it sent them, on the thread that
   * polls, and every record polled before N on that partition went into this batch or an earlier one. Only partitions in
   * the consumer's current assignment are committed; offsets for others are dropped, so their records are redelivered to
   * the partition's new owner. A failed commit is logged and its offsets kept, to be retried with the next batch or on
   * close.
   */
  @Override
  public void batchComplete(List<Document> batch) throws Exception {
    if (!commitOnBatchComplete) {
      return;
    }
    for (Document doc : batch) {
      if (doc instanceof KafkaDocument kDoc) {
        pendingOffsets.merge(new TopicPartition(kDoc.getTopic(), kDoc.getPartition()), kDoc.getOffset() + 1, Math::max);
      }
    }
    try {
      commitPending();
    } catch (InterruptException e) {
      throw e;
    } catch (KafkaException e) {
      log.warn("Could not commit offsets {}; retrying with the next batch.", pendingOffsets, e);
    }
  }

  // Commits pending offsets of assigned partitions and drops the rest. Keeps them if the commit fails.
  private void commitPending() {
    pendingOffsets.keySet().retainAll(destConsumer.assignment());
    if (pendingOffsets.isEmpty()) {
      return;
    }
    Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
    pendingOffsets.forEach((partition, offset) -> toCommit.put(partition, new OffsetAndMetadata(offset)));
    destConsumer.commitSync(toCommit);
    pendingOffsets.clear();
  }

  // Whether the consumer built from this config auto-commits, resolving an unset value to the client's default.
  private static boolean isAutoCommitEnabled(Config config) {
    Object value = KafkaUtils.createConsumerProps(config, "validation").get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
    if (value == null) {
      value = ConsumerConfig.configDef().defaultValues().get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
    }
    return Boolean.parseBoolean(String.valueOf(value));
  }

}
