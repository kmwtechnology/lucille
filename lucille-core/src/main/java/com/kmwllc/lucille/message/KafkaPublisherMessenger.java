package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaPublisherMessenger implements PublisherMessenger {

  private static final Logger log = LoggerFactory.getLogger(KafkaPublisherMessenger.class);

  private final Config config;
  private final AtomicReference<Exception> sendException = new AtomicReference<>();
  private KafkaProducer<String, Document> kafkaProducer;
  private Consumer<String, String> eventConsumer;
  private String runId;
  private String pipelineName;

  public KafkaPublisherMessenger(Config config) {
    this.config = config;
  }

  public void initialize(String runId, String pipelineName) throws Exception {
    if (this.runId != null) {
      throw new Exception("Already initialized.");
    }
    this.runId = runId;
    this.pipelineName = pipelineName;

    // create event topic explicitly with one partition
    KafkaUtils.createEventTopic(config, pipelineName, runId);

    String kafkaClientId = "com.kmwllc.lucille-publisher-" + pipelineName;
    String eventTopicName = KafkaUtils.getEventTopicName(config, pipelineName, runId);
    this.eventConsumer = KafkaUtils.createEventConsumer(config, kafkaClientId);
    this.eventConsumer.subscribe(Collections.singletonList(eventTopicName));
    this.kafkaProducer = KafkaUtils.createDocumentProducer(config);
  }

  @Override
  public String getRunId() {
    return runId;
  }

  public void sendForProcessing(Document document) throws Exception {
    checkException();
    kafkaProducer.send(
        new ProducerRecord<>(KafkaUtils.getSourceTopicName(pipelineName, config), document.getId(), document),
        (metadata, exception) -> {
          if (exception != null) {
            log.error("Kafka send failed for document: {}", document.getId(), exception);
            sendException.compareAndSet(null, exception);
          }
        });
  }

  @Override
  public void flush() throws Exception {
    kafkaProducer.flush();
    checkException();
  }

  private void checkException() throws Exception {
    Exception e = sendException.get();
    if (e != null) {
      throw new Exception("Kafka send failed", e);
    }
  }

  /**
   * Polls for an Event that is waiting to be consumed.
   */
  @Override
  public Event pollEvent() throws Exception {
    ConsumerRecords<String, String> consumerRecords = eventConsumer.poll(KafkaUtils.POLL_INTERVAL);
    KafkaUtils.validateAtMostOneRecord(consumerRecords);
    if (consumerRecords.count() > 0) {
      // we do not call commitSync() or commitAsync() on the eventConsumer here because auto-commit is enabled
      // for the event topic. With auto-commmit enabled, the behavior is similar to commitAsync(), which is important for
      // achieving good throughput when the Publisher has a large backlog of events to process.

      ConsumerRecord<String, String> record = consumerRecords.iterator().next();
      return Event.fromJsonString(record.value());
    }
    return null;
  }

  public void close() {
    if (kafkaProducer != null) {
      try {
        kafkaProducer.close();
      } catch (Exception e) {
        log.error("Couldn't close kafka producer", e);
      }
    }
    if (eventConsumer != null) {
      try {
        eventConsumer.close();
      } catch (Exception e) {
        log.error("Couldn't close kafka event consumer", e);
      }
    }
  }

}
