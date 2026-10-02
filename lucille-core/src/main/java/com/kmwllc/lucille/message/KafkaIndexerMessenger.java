package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.KafkaDocument;
import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * IndexerMessenger that consumes Documents from a pipeline's destination topic and sends Events to Kafka.
 *
 * <p> <b>Offset commits</b> follow a {@link KafkaCommitPolicy} chosen by indexer.maxConcurrentBatches (see {@link Indexer}):
 * <ul>
 *   <li>1 (the default): each record's offset is committed as soon as it is polled, before it is indexed. A crash can
 *   lose about one batch.</li>
 *   <li>Greater than 1: the indexer keeps polling while batches are in flight, so committing at poll could lose all of
 *   them. Instead, offsets are committed once their batch completes, at the start of the next poll. Delivery is then
 *   at-least-once: a crash or rebalance redelivers in-flight documents, which may be indexed twice and get duplicate
 *   completion events, but never loses them. When the topic is idle, a completed batch's events and commit can wait up to
 *   one poll interval ({@link KafkaUtils#POLL_INTERVAL}).</li>
 * </ul>
 *
 * <p> In both cases, kafka.maxPollIntervalSecs must exceed the longest time the indexer can go without polling: one
 * batch's send with its retries or, for a batch containing a delete-by-query, the sends of the batches ahead of it plus
 * its own.
 */
public class KafkaIndexerMessenger implements IndexerMessenger {

  private static final Logger log = LoggerFactory.getLogger(KafkaIndexerMessenger.class);
  private final Consumer<String, KafkaDocument> destConsumer;
  private final KafkaCommitPolicy commitPolicy;
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
    this.destConsumer.subscribe(Collections.singletonList(KafkaUtils.getDestTopicName(pipelineName)));
    this.commitPolicy = KafkaCommitPolicy.forMaxConcurrentBatches(Indexer.getMaxConcurrentBatches(config), destConsumer);
    this.kafkaEventProducer = KafkaUtils.createEventProducer(config);
    this.config = config;
  }

  /**
   * Polls for a document that has been processed by the pipeine and is waiting to be indexed.
   */
  @Override
  public Document pollDocToIndex() throws Exception {
    commitPolicy.beforePoll();
    ConsumerRecords<String, KafkaDocument> consumerRecords = destConsumer.poll(KafkaUtils.POLL_INTERVAL);
    KafkaUtils.validateAtMostOneRecord(consumerRecords);
    if (consumerRecords.count() > 0) {
      commitPolicy.afterRecordPolled();
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
    commitPolicy.close();
    destConsumer.close();
  }

  /** Lets the commit policy record the batch's offsets; any commit happens at the next poll. */
  @Override
  public void batchComplete(List<Document> batch) throws Exception {
    commitPolicy.batchComplete(batch);
  }

}
