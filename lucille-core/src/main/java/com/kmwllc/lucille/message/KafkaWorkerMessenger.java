package com.kmwllc.lucille.message;

import com.codahale.metrics.Timer;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.EventFieldConfig;
import com.kmwllc.lucille.core.KafkaDocument;
import com.typesafe.config.Config;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RecordDeserializationException;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;

public class KafkaWorkerMessenger implements WorkerMessenger {

  private static final Logger log = LoggerFactory.getLogger(KafkaWorkerMessenger.class);
  private final Consumer<String, KafkaDocument> sourceConsumer;
  private final KafkaProducer<String, Document> kafkaDocumentProducer;
  private final KafkaProducer<String, String> kafkaEventProducer;
  private final Config config;
  private final Duration pollInterval;
  private final String pipelineName;
  private final EventFieldConfig eventFieldConfig;
  private final DeserializationErrorHandler deserializationErrorHandler;
  private final KafkaConsumerMetrics consumerMetrics;

  public KafkaWorkerMessenger(Config config, String pipelineName) {
    this(config, pipelineName, createSourceConsumer(config, pipelineName));
  }

  private static Consumer<String, KafkaDocument> createSourceConsumer(Config config, String pipelineName) {
    // append random string to kafka client ID to prevent kafka from issuing a warning when multiple consumers
    // with the same client ID are started in separate worker threads
    String kafkaClientId = "com.kmwllc.lucille-worker-" + pipelineName + "-" + RandomStringUtils.randomAlphanumeric(8);
    return KafkaUtils.createDocumentConsumer(config, kafkaClientId);
  }

  // takes the source consumer, so tests can supply one
  KafkaWorkerMessenger(Config config, String pipelineName, Consumer<String, KafkaDocument> sourceConsumer) {
    this.config = config;
    this.pollInterval = KafkaUtils.getPollInterval(config);
    this.pipelineName = pipelineName;
    this.eventFieldConfig = EventFieldConfig.fromConfig(config);
    this.kafkaDocumentProducer = KafkaUtils.createDocumentProducer(config);
    this.kafkaEventProducer = KafkaUtils.createEventProducer(config);
    this.sourceConsumer = sourceConsumer;
    this.sourceConsumer.subscribe(Collections.singletonList(KafkaUtils.getSourceTopicName(pipelineName, config)));
    this.deserializationErrorHandler =
        new DeserializationErrorHandler(config, sourceConsumer);
    this.consumerMetrics = KafkaConsumerMetrics.forWorker(pipelineName);
  }

  /**
   * Polls for a document that is waiting to be processed by the pipeline. The poll is timed (see
   * {@link KafkaConsumerMetrics}).
   */
  @Override
  public Document pollDocToProcess() throws MessengerException {
    ConsumerRecords<String, KafkaDocument> consumerRecords;
    try (Timer.Context ignored = consumerMetrics.timePoll()) {
      consumerRecords = sourceConsumer.poll(pollInterval);
    } catch (RecordDeserializationException e) {
      // throws unless configured to skip; the Worker commits the advanced position when we return null
      deserializationErrorHandler.handleOrThrow(e);
      return null;
    }
    KafkaUtils.validateAtMostOneRecord(consumerRecords);
    if (consumerRecords.count() > 0) {
      ConsumerRecord<String, KafkaDocument> record = consumerRecords.iterator().next();
      deserializationErrorHandler.onSuccessfulPoll(record);
      KafkaDocument doc = record.value();
      doc.setKafkaMetadata(record.topic(), record.partition(), record.offset(), record.key());
      return doc;
    }
    return null;
  }

  @Override
  public void commitPendingDocOffsets() {
    // Called after the documents made from the polled record have been sent to the destination topic, each send waiting for
    // the broker's acknowledgment, so the record is safe to commit. The commit is synchronous, so that a worker crash or a
    // consumer group rebalance re-processes as few records as possible. The commit is timed, and counted if it throws.
    consumerMetrics.commit(() -> sourceConsumer.commitSync());
  }

  /**
   * Sends a processed document to the appropriate destination for documents waiting to be indexed.
   *
   */
  @Override
  public void sendForIndexing(Document document) throws MessengerException {
    try {
      kafkaDocumentProducer.send(
          new ProducerRecord<>(KafkaUtils.getDestTopicName(pipelineName), document.getId(), document)).get();
    } catch (ExecutionException e) {
      throw KafkaUtils.toMessengerException("Sending document " + document.getId() + " for indexing", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new MessengerException("Sending document " + document.getId() + " for indexing was interrupted", e);
    }
  }

  public void sendFailed(Document document) throws MessengerException {
    KafkaUtils.sendToFailTopic(kafkaDocumentProducer, pipelineName, document);
  }

  @Override
  public void sendEvent(Document document, String message, Event.Type type) throws MessengerException {
    if (kafkaEventProducer == null) {
      return;
    }
    Event event = new Event(document, message, type, eventFieldConfig);
    sendEvent(event);
  }

  /**
   * Sends an Event relating to a Document to the appropriate location for Events.
   *
   */
  @Override
  public void sendEvent(Event event) throws MessengerException {
    if (kafkaEventProducer == null) {
      return;
    }
    String confirmationTopicName = KafkaUtils.getEventTopicName(config, pipelineName, event.getRunId());
    try {
      kafkaEventProducer.send(
          new ProducerRecord<>(confirmationTopicName, event.getDocumentId(), event.toString())).get();
    } catch (ExecutionException e) {
      throw KafkaUtils.toMessengerException("Sending " + event.getType() + " event for document " + event.getDocumentId(), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new MessengerException("Sending " + event.getType() + " event for document " + event.getDocumentId() + " was interrupted", e);
    }
  }

  @Override
  public void close() {
    if (sourceConsumer != null) {
      sourceConsumer.close();
    }
    if (kafkaDocumentProducer != null) {
      kafkaDocumentProducer.close();
    }
    if (kafkaEventProducer != null) {
      kafkaEventProducer.close();
    }
  }

}
