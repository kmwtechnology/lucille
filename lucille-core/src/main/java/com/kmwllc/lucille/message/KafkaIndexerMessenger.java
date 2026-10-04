package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.KafkaDocument;
import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaIndexerMessenger implements IndexerMessenger {

  private static final Logger log = LoggerFactory.getLogger(KafkaIndexerMessenger.class);
  private final Consumer<String, KafkaDocument> destConsumer;
  private final KafkaProducer<String, String> kafkaEventProducer;
  private final String pipelineName;
  private final Config config;

  // The fields below support safe commit-on-completion across consumer-group rebalances. They are only accessed on the
  // indexer thread: pollDocToIndex, batchComplete, and close all run there, and the rebalance listener is invoked by
  // the consumer from within poll() or close(), which are only called on that thread.

  // Per assigned partition, the offset after the last record returned by pollDocToIndex in the current assignment.
  private final Map<TopicPartition, Long> polledThrough = new HashMap<>();
  // Per partition revoked or lost, the polled-through offset at the time it was taken away. A batch whose commit offset
  // is at or below this holds records from an earlier assignment and must not be committed (another consumer may have
  // moved past it).
  private final Map<TopicPartition, Long> staleThrough = new HashMap<>();
  // Offsets of completed batches not yet committed successfully; retained and retried if a commit fails.
  private final Map<TopicPartition, OffsetAndMetadata> pendingOffsets = new HashMap<>();

  public KafkaIndexerMessenger(Config config, String pipelineName) {
    this(config, pipelineName, KafkaUtils.createDocumentConsumer(config, "com.kmwllc.lucille-indexer-" + pipelineName));
  }

  // For tests: allows a MockConsumer (or other test double) to be supplied in place of the real Kafka consumer.
  KafkaIndexerMessenger(Config config, String pipelineName, Consumer<String, KafkaDocument> destConsumer) {
    this.pipelineName = pipelineName;
    this.destConsumer = destConsumer;
    this.destConsumer.subscribe(Collections.singletonList(KafkaUtils.getDestTopicName(pipelineName)), new RebalanceListener());
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
      // Offsets are not committed here. Committing at poll, before the document is indexed, is at-most-once: a crash
      // after the commit but before indexing would skip the document entirely. Instead the offset is committed in
      // batchComplete, once the document has been indexed and its events emitted (at-least-once).
      ConsumerRecord<String, KafkaDocument> record = consumerRecords.iterator().next();
      // Record how far we have polled this partition in the current assignment, so a later rebalance can tell which
      // completed batches hold records from this assignment (safe to commit) versus an earlier one (stale).
      polledThrough.put(new TopicPartition(record.topic(), record.partition()), record.offset() + 1);
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
    // Commit offsets of batches completed but not yet committed, so a clean shutdown does not force their re-delivery.
    if (!pendingOffsets.isEmpty()) {
      try {
        commitPending();
      } catch (Exception e) {
        log.warn("Could not commit completed-batch offsets on close; those documents will be re-delivered.", e);
      }
    }
    destConsumer.close();
  }

  /**
   * Commits the destination-topic offsets of a completed batch. For each partition in the batch, the offset after that
   * partition's last record (the next offset to read) is recorded as pending, then all pending offsets are committed.
   *
   * <p> Why this cannot lose a document: the Indexer completes batches in dispatch order, and every record this consumer
   * polled before the last record of this batch went into this batch or an earlier one. So when this batch completes,
   * every record below the committed offset has been indexed; a crash re-reads from the committed offset and loses
   * nothing.
   *
   * <p> Why this cannot rewind another consumer's commit: only partitions still assigned to this consumer are committed,
   * and offsets of records polled under an earlier assignment of a partition (revoked and possibly reassigned since) are
   * dropped, since another consumer may have committed further. Dropping an offset only re-delivers its records.
   *
   * <p> A failed commit is retained in pendingOffsets and retried on the next completed batch, on revocation, and on
   * close, so the records are re-delivered rather than lost.
   */
  @Override
  public void batchComplete(List<Document> batch) throws Exception {
    if (batch.isEmpty()) {
      return;
    }
    for (Document doc : batch) {
      if (!(doc instanceof KafkaDocument)) {
        continue;
      }
      KafkaDocument kafkaDoc = (KafkaDocument) doc;
      TopicPartition partition = new TopicPartition(kafkaDoc.getTopic(), kafkaDoc.getPartition());
      // The committed offset is the offset of the next record to read, so add one to the last record processed.
      long nextOffset = kafkaDoc.getOffset() + 1;
      // Skip records polled under an earlier assignment of this partition: committing them could rewind a consumer that
      // owns the partition now.
      Long stale = staleThrough.get(partition);
      if (stale != null && nextOffset <= stale) {
        continue;
      }
      OffsetAndMetadata current = pendingOffsets.get(partition);
      if (current == null || current.offset() < nextOffset) {
        pendingOffsets.put(partition, new OffsetAndMetadata(nextOffset));
      }
    }
    commitPending();
  }

  // Commits pending offsets for partitions still assigned, dropping the rest. Keeps them if the commit fails, so they
  // are retried later.
  private void commitPending() {
    pendingOffsets.keySet().retainAll(destConsumer.assignment());
    if (pendingOffsets.isEmpty()) {
      return;
    }
    Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>(pendingOffsets);
    destConsumer.commitSync(toCommit);
    toCommit.forEach(pendingOffsets::remove);
  }

  // Invoked by the consumer on the indexer thread, from within poll() or close().
  private class RebalanceListener implements ConsumerRebalanceListener {

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      // The partitions are still owned during this callback, so offsets of batches already completed can still be
      // committed before they are handed off.
      Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
      for (TopicPartition partition : partitions) {
        OffsetAndMetadata offset = pendingOffsets.get(partition);
        if (offset != null) {
          toCommit.put(partition, offset);
        }
      }
      if (!toCommit.isEmpty()) {
        try {
          destConsumer.commitSync(toCommit);
        } catch (Exception e) {
          log.warn("Could not commit offsets {} on revocation; those documents will be re-delivered.", toCommit, e);
        }
      }
      forget(partitions);
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
      // The partitions may already be owned by another consumer, so nothing is committed.
      forget(partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    }

    // Records polled under the current assignment of these partitions become stale, and any pending offset is dropped.
    private void forget(Collection<TopicPartition> partitions) {
      for (TopicPartition partition : partitions) {
        pendingOffsets.remove(partition);
        Long polled = polledThrough.remove(partition);
        if (polled != null) {
          staleThrough.merge(partition, polled, Math::max);
        }
      }
    }
  }

}
