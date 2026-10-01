package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.Indexer;
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

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * IndexerMessenger that consumes Documents from a pipeline's destination topic and sends Events to Kafka.
 *
 * <p> <b>Offset commits.</b> How offsets on the destination topic are committed depends on indexer.maxConcurrentBatches
 * (see {@link Indexer}):
 * <ul>
 *   <li>When it is 1 (the default), the offset of each Document is committed as soon as the Document is polled, before it
 *   is indexed. The indexer stops polling while it sends a batch, so a crash can lose (never index) at most about one
 *   batch.</li>
 *   <li>When it is greater than 1, the indexer keeps polling while several batches are in flight, so committing at poll
 *   would let a crash lose every in-flight batch. Instead, offsets are committed in {@link #batchComplete(List)}, after a
 *   batch has been sent and its events emitted. Delivery is then <b>at-least-once</b>: nothing is lost on a crash or a
 *   consumer group rebalance, but Documents that were polled or indexed and not yet committed are delivered again (to this
 *   or another indexer), so they may be indexed twice and may receive more than one completion Event. The poll timeout is
 *   also shortened so that completed batches are processed promptly when the topic is idle, and {@link #keepAlive()}
 *   polls the consumer without consuming records so that it stays in its group while the indexer waits on in-flight
 *   batches.</li>
 * </ul>
 */
public class KafkaIndexerMessenger implements IndexerMessenger {

  private static final Logger log = LoggerFactory.getLogger(KafkaIndexerMessenger.class);

  // poll timeout when batches are completed asynchronously; matches the local messengers so completions are not delayed
  static final Duration CONCURRENT_POLL_INTERVAL = Duration.ofMillis(LocalMessenger.POLL_TIMEOUT_MS);

  private final Consumer<String, KafkaDocument> destConsumer;
  private final KafkaProducer<String, String> kafkaEventProducer;
  private final String pipelineName;
  private final Config config;

  // true when indexer.maxConcurrentBatches > 1: offsets are committed on batch completion instead of at poll
  private final boolean commitOnComplete;

  // The fields below are only used when commitOnComplete is true. They are only accessed on the indexer thread: the
  // rebalance listener is invoked by the consumer from within poll() (or close()), which is only called on that thread.

  // per assigned partition, the offset after the last record returned by pollDocToIndex in the current assignment
  private final Map<TopicPartition, Long> polledThrough = new HashMap<>();
  // per partition that has been revoked or lost, the offset after the last record polled before it was taken away.
  // Batches whose commit offset is at or below this value hold records from an earlier assignment and are not committed.
  private final Map<TopicPartition, Long> staleThrough = new HashMap<>();
  // offsets of completed batches that have not yet been committed successfully
  private final Map<TopicPartition, OffsetAndMetadata> pendingOffsets = new HashMap<>();

  public KafkaIndexerMessenger(Config config, String pipelineName) {
    this(config, pipelineName,
        KafkaUtils.createDocumentConsumer(config, "com.kmwllc.lucille-indexer-" + pipelineName));
  }

  // for tests: allows a MockConsumer to be supplied
  KafkaIndexerMessenger(Config config, String pipelineName, Consumer<String, KafkaDocument> destConsumer) {
    this.pipelineName = pipelineName;
    this.commitOnComplete = Indexer.getMaxConcurrentBatches(config) > 1;
    this.destConsumer = destConsumer;
    List<String> topics = Collections.singletonList(KafkaUtils.getDestTopicName(pipelineName));
    if (commitOnComplete) {
      this.destConsumer.subscribe(topics, new RebalanceListener());
    } else {
      this.destConsumer.subscribe(topics);
    }
    this.kafkaEventProducer = KafkaUtils.createEventProducer(config);
    this.config = config;
  }

  /**
   * Polls for a document that has been processed by the pipeine and is waiting to be indexed.
   */
  @Override
  public Document pollDocToIndex() throws Exception {
    // undo any pause applied by keepAlive(); paused() only reports partitions that are still assigned
    Set<TopicPartition> paused = destConsumer.paused();
    if (!paused.isEmpty()) {
      destConsumer.resume(paused);
    }

    ConsumerRecords<String, KafkaDocument> consumerRecords =
        destConsumer.poll(commitOnComplete ? CONCURRENT_POLL_INTERVAL : KafkaUtils.POLL_INTERVAL);
    KafkaUtils.validateAtMostOneRecord(consumerRecords);
    if (consumerRecords.count() > 0) {
      ConsumerRecord<String, KafkaDocument> record = consumerRecords.iterator().next();
      if (commitOnComplete) {
        polledThrough.put(new TopicPartition(record.topic(), record.partition()), record.offset() + 1);
      } else {
        // offsets are committed synchronously to ensure that offsets are successfully committed and to reduce the likelihood of duplicate events being sent to the event topic.
        // This reduces the number of documents that might be reindexed in the event of an indexer crash/restart or in the case of a consumer group reblance.
        destConsumer.commitSync();
      }
      KafkaDocument doc = record.value();
      doc.setKafkaMetadata(record);
      return doc;
    }
    return null;
  }

  /**
   * Polls the consumer so that it remains in its consumer group, without delivering any records or moving the position
   * from which {@link #pollDocToIndex()} continues. All assigned partitions are paused first, so the poll returns no
   * records from them; they are resumed by the next pollDocToIndex(). A partition newly assigned during this poll is not
   * paused and may return a record, so the consumer seeks back to the first record returned for each partition, and that
   * record is delivered by a later pollDocToIndex() instead.
   */
  @Override
  public void keepAlive() throws Exception {
    destConsumer.pause(destConsumer.assignment());
    ConsumerRecords<String, KafkaDocument> consumerRecords = destConsumer.poll(Duration.ZERO);
    for (TopicPartition partition : consumerRecords.partitions()) {
      List<ConsumerRecord<String, KafkaDocument>> records = consumerRecords.records(partition);
      if (!records.isEmpty()) {
        destConsumer.seek(partition, records.get(0).offset());
      }
    }
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
    if (commitOnComplete && !pendingOffsets.isEmpty()) {
      try {
        commitPending();
      } catch (Exception e) {
        log.warn("Could not commit offsets of completed batches on close; those documents will be delivered again.", e);
      }
    }
    destConsumer.close();
  }

  // for tests: the partitions currently assigned to this messenger's consumer
  Set<TopicPartition> assignment() {
    return destConsumer.assignment();
  }

  /**
   * When indexer.maxConcurrentBatches is greater than 1, commits, for each partition, the offset after the last record of
   * the batch. Otherwise does nothing, since offsets were committed at poll.
   *
   * <p> Why this cannot lose a Document: a committed offset N on a partition is only safe if every record below N has been
   * indexed. A consumer starts reading a partition at its committed offset, which by induction is safe, and reads it in
   * order. The Indexer completes batches in the order it dispatched them, and every record this consumer polled before
   * the last record of this batch went into this batch or an earlier one. So when this batch completes, every record
   * below N has completed, and committing N is safe. (Known exception: with indexer.indexOverrideField, documents are
   * batched per index, and one index's batch can be flushed, and so completed, while a document polled earlier waits in
   * another index's unflushed batch, so the commit can pass that document.)
   *
   * <p> Why it cannot rewind another consumer's commit: only partitions currently assigned to this consumer are committed,
   * and offsets of records polled under an earlier assignment of a partition (one that was revoked or lost and possibly
   * reassigned since) are dropped, since another consumer may have committed further in the meantime. Dropping an
   * offset only means the records are delivered again.
   *
   * <p> A failed commit (CommitFailedException, RebalanceInProgressException, a timeout, ...) is rethrown, and the
   * Indexer logs it. The committed offset then stays where it was, so the batch's records are delivered again rather
   * than lost. The offsets are kept and retried with the next completed batch, on revocation, and on close.
   */
  @Override
  public void batchComplete(List<Document> batch) throws Exception {
    if (!commitOnComplete || batch.isEmpty()) {
      return;
    }
    for (Document doc : batch) {
      if (!(doc instanceof KafkaDocument)) {
        continue;
      }
      KafkaDocument kDoc = (KafkaDocument) doc;
      TopicPartition partition = new TopicPartition(kDoc.getTopic(), kDoc.getPartition());
      // the committed offset is the offset of the next record to read, so add one to the last record processed
      long next = kDoc.getOffset() + 1;
      Long stale = staleThrough.get(partition);
      if (stale != null && next <= stale) {
        continue;
      }
      OffsetAndMetadata current = pendingOffsets.get(partition);
      if (current == null || current.offset() < next) {
        pendingOffsets.put(partition, new OffsetAndMetadata(next));
      }
    }
    commitPending();
  }

  // Commits pending offsets for partitions that are still assigned, dropping the rest. Keeps them if the commit fails.
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
      // The partitions are still owned during this callback, so offsets of completed batches can still be committed.
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
          log.warn("Could not commit offsets {} on revocation; those documents will be delivered again.", toCommit, e);
        }
      }
      forget(partitions);
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
      // the partitions may already be owned by another consumer, so nothing is committed
      forget(partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    }

    // Records from the current assignment of these partitions become stale, and their pending offsets are dropped.
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
