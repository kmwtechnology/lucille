package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.KafkaDocument;
import com.typesafe.config.Config;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

public class HybridWorkerMessenger implements WorkerMessenger {

  private static final Logger log = LoggerFactory.getLogger(KafkaWorkerMessenger.class);
  private final Consumer<String, KafkaDocument> sourceConsumer;
  private final KafkaProducer<String, String> kafkaEventProducer;
  private final LinkedBlockingQueue<Document> pipelineDest;
  private final LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets;

  private final Config config;
  private final String pipelineName;

  public HybridWorkerMessenger(Config config, String pipelineName,
      LinkedBlockingQueue<Document> pipelineDest,
      LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets,
      KafkaConsumer sourceConsumer) {
    this.config = config;
    this.pipelineName = pipelineName;
    this.pipelineDest = pipelineDest;
    this.offsets = offsets;
    this.sourceConsumer = sourceConsumer;
    this.kafkaEventProducer = KafkaUtils.createEventProducer(config);
  }

  public HybridWorkerMessenger(Config config, String pipelineName,
      LinkedBlockingQueue<Document> pipelineDest,
      LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets) {
    this(config, pipelineName, pipelineDest, offsets, createSourceConsumer(config, pipelineName));
  }

  private static KafkaConsumer createSourceConsumer(Config config, String pipelineName) {
    // append random string to kafka client ID to prevent kafka from issuing a warning when multiple consumers
    // with the same client ID are started in separate worker threads
    String kafkaClientId = "com.kmwllc.lucille-worker-" + pipelineName + "-" + RandomStringUtils.randomAlphanumeric(8);
    KafkaConsumer consumer = KafkaUtils.createDocumentConsumer(config, kafkaClientId);
    String sourceTopicName = KafkaUtils.getSourceTopicName(pipelineName, config);
    consumer.subscribe(Pattern.compile(sourceTopicName));

    return consumer;
  }

  /**
   * Polls for a document that is waiting to be processed by the pipeline.
   *
   * Does not commit offsets.
   */
  @Override
  public KafkaDocument pollDocToProcess() throws Exception {
    ConsumerRecords<String, KafkaDocument> consumerRecords = sourceConsumer.poll(KafkaUtils.POLL_INTERVAL);
    KafkaUtils.validateAtMostOneRecord(consumerRecords);
    if (consumerRecords.count() > 0) {
      ConsumerRecord<String, KafkaDocument> record = consumerRecords.iterator().next();
      KafkaDocument doc = record.value();
      doc.setKafkaMetadata(record);
      return doc;
    }
    return null;
  }

  /**
   * Commits the offsets of all indexer batches completed since the last call, using a single commitSync that keeps
   * only the highest offset per partition.
   *
   * <p>A committed offset is a cumulative watermark: committing N means records 0..N-1 of that partition are done.
   * Committing the highest drained offset therefore has the same effect as committing each drained map in turn, which
   * is what this method did before, and it is safe for the same reasons. Within a WorkerIndexer:
   * <ol>
   *   <li>the source consumer reads one record per poll, in offset order, and this worker processes them one at a
   *   time;</li>
   *   <li>the single indexer thread queues (last offset + 1) for each partition in a batch only once that batch has
   *   been handled: sent to the destination, or reported with FAIL events if sending failed;</li>
   *   <li>one indexer thread queues the maps and one worker thread drains them, so a partition's offsets are queued
   *   in increasing order.</li>
   * </ol>
   * So a map holding offset N for a partition implies every lower offset was already handled. Taking the max also
   * means a stale lower map can never move a partition's committed offset backwards.
   *
   * <p>This relies on (3). If indexer batches ever complete out of order (for example, sent concurrently), a higher
   * offset could be queued while a lower batch is still in flight, and committing it, whether merged or one map at a
   * time, could skip records that are never indexed. Revisit this method if that ordering changes.
   *
   * <p>If the commit fails, the drained offsets are not re-queued; as offsets are cumulative per partition, the
   * next completed batch supersedes them and the cost is at most some reprocessing after a restart.
   */
  @Override
  public void commitPendingDocOffsets() throws Exception {
    Map<TopicPartition, OffsetAndMetadata> batchOffsets = offsets.poll();
    if (batchOffsets == null) {
      return;
    }
    Map<TopicPartition, OffsetAndMetadata> highestOffsets = new HashMap<>();
    while (batchOffsets != null) {
      batchOffsets.forEach((partition, offset) -> highestOffsets.merge(partition, offset,
          (existing, candidate) -> candidate.offset() > existing.offset() ? candidate : existing));
      batchOffsets = offsets.poll();
    }
    // offsets are committed synchronously, and only after the indexer has sent the batch, so a successful commit means
    // those documents are already in the destination. Doing it synchronously limits how much is reprocessed and
    // reindexed after a HybridWorker crash/restart or a consumer group rebalance.
    sourceConsumer.commitSync(highestOffsets);
  }

  /**
   * Sends a processed document to the appropriate destination for documents waiting to be indexed.
   *
   */
  @Override
  public void sendForIndexing(Document document) throws Exception {
    pipelineDest.put(document);
  }

  @Override
  public void sendFailed(Document document) throws Exception {
  }

  @Override
  public void sendEvent(Event event) throws Exception {
    if (kafkaEventProducer == null) {
      return;
    }
    String confirmationTopicName = KafkaUtils.getEventTopicName(config, pipelineName, event.getRunId());
    RecordMetadata result = kafkaEventProducer.send(
        new ProducerRecord<>(confirmationTopicName, event.getDocumentId(), event.toString())).get();
  }

  @Override
  public void sendEvent(Document document, String message, Event.Type type) throws Exception {
    if (kafkaEventProducer == null) {
      return;
    }
    Event event = new Event(document, message, type);
    sendEvent(event);
  }

  @Override
  public void close() throws Exception {
    if (sourceConsumer != null) {
      sourceConsumer.close();
    }
    if (kafkaEventProducer != null) {
      kafkaEventProducer.close();
    }
  }

}

