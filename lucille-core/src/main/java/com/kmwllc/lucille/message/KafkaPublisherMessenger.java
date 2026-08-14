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

public class KafkaPublisherMessenger implements PublisherMessenger {

  private static final Logger log = LoggerFactory.getLogger(KafkaPublisherMessenger.class);

  private final Config config;
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
    this.kafkaProducer = KafkaUtils.createDocumentProducer(config, kafkaClientId + "-documents");
  }

  @Override
  public String getRunId() {
    return runId;
  }

  /**
   * Blocks until the record has been acked. Prefer the callback overload: this one pins the calling
   * thread's throughput at one document per broker round trip.
   */
  public void sendForProcessing(Document document) throws Exception {
    // No flush() here. flush() is producer-global: it blocks the calling thread until every record
    // buffered by every *other* publishing thread has been acked, which turns concurrent publishing
    // into a convoy whose cost grows with the thread count. It is also redundant for durability --
    // the get() below has already blocked until this record was acked by all in-sync replicas.
    // Records still in the accumulator at end of run are flushed by KafkaProducer.close().
    kafkaProducer.send(sourceRecord(document)).get();
  }

  @Override
  public void sendForProcessing(Document document, SendCallback callback) throws Exception {
    // The calling thread returns once the record is in the accumulator; the producer's sender thread
    // batches it with whatever else is queued and reports the outcome to the callback. send() still
    // blocks if the accumulator is full, bounded by max.block.ms -- that is the backpressure signal.
    kafkaProducer.send(sourceRecord(document), (metadata, exception) -> callback.onCompletion(exception));
  }

  private ProducerRecord<String, Document> sourceRecord(Document document) {
    return new ProducerRecord<>(
        KafkaUtils.getSourceTopicName(pipelineName, config), document.getId(), document);
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
