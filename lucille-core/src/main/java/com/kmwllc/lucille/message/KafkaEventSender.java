package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Event;
import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends Events to a run's Kafka event topic without waiting for each one to be acknowledged.
 *
 * <p>{@link #send(Event)} hands the Event to the producer and returns. Waiting on every send costs at least the
 * producer's linger.ms plus a broker round trip per Event, which adds up when a pipeline emits many children or an
 * indexer batch fails. Callers instead call {@link #flush()} at the points where delivery matters: before a parent
 * document is handed on (so its children's CREATE events reach the event topic before any terminal event for the
 * parent can), and before the offsets of the documents the Events describe are committed.
 *
 * <p>Lucille creates the event topic with a single partition and the producer is idempotent, so Events from one
 * sender are delivered in the order they were sent. Flushing orders them against Events from other producers.
 *
 * <p>Each messenger owns one sender and uses it from one thread.
 */
final class KafkaEventSender {

  private static final Logger log = LoggerFactory.getLogger(KafkaEventSender.class);
  private static final int MAX_IDS_IN_MESSAGE = 20;

  // null when events are disabled (kafka.events: false)
  private final Producer<String, String> producer;
  private final Config config;
  private final String pipelineName;

  // set by send(), cleared by flush(): lets flush() skip the producer when nothing was sent since the last one
  private final AtomicBoolean unflushed = new AtomicBoolean(false);

  // failures reported by producer callbacks since the last flush(); callbacks run on the producer's I/O thread
  private final List<String> failedEvents = new ArrayList<>();
  private Exception firstFailure;

  KafkaEventSender(Config config, String pipelineName) {
    this.producer = KafkaUtils.createEventProducer(config);
    this.config = config;
    this.pipelineName = pipelineName;
  }

  boolean isEnabled() {
    return producer != null;
  }

  /**
   * Hands the Event to the producer without waiting for it to be acknowledged. A delivery failure is logged with the
   * Event's document ID and reported by the next {@link #flush()}.
   */
  void send(Event event) {
    if (producer == null) {
      return;
    }
    unflushed.set(true);
    String topic = KafkaUtils.getEventTopicName(config, pipelineName, event.getRunId());
    producer.send(new ProducerRecord<>(topic, event.getDocumentId(), event.toString()), (metadata, exception) -> {
      if (exception != null) {
        log.error("Failed to send {} event for doc: {}", event.getType(), event.getDocumentId(), exception);
        recordFailure(event, exception);
      }
    });
  }

  /**
   * Blocks until every Event sent so far has been acknowledged or has failed. Throws if any Event sent since the
   * previous flush failed, naming those Events' documents; each failure is reported once.
   */
  void flush() throws Exception {
    if (producer == null || !unflushed.getAndSet(false)) {
      return;
    }
    RuntimeException flushError = null;
    try {
      // flush() returns only after every earlier send has completed, including its callback
      producer.flush();
    } catch (RuntimeException e) {
      // sends may still be in flight (e.g. the flush was interrupted): make the next flush wait for them
      unflushed.set(true);
      flushError = e;
    }
    // take recorded failures even if the flush was interrupted, so they are not reported against a later document
    Exception failure = takeFailures();
    if (flushError != null) {
      if (failure != null) {
        flushError.addSuppressed(failure);
      }
      throw flushError;
    }
    if (failure != null) {
      throw failure;
    }
  }

  /**
   * Closes the producer, which first delivers any Events not yet flushed.
   */
  void close() {
    if (producer != null) {
      producer.close();
    }
  }

  private synchronized void recordFailure(Event event, Exception exception) {
    failedEvents.add(event.getType() + " " + event.getDocumentId());
    if (firstFailure == null) {
      firstFailure = exception;
    }
  }

  private synchronized Exception takeFailures() {
    if (failedEvents.isEmpty()) {
      return null;
    }
    // each failure is also logged individually by its callback; keep the summary short when a whole batch fails
    List<String> shown = failedEvents.size() > MAX_IDS_IN_MESSAGE
        ? failedEvents.subList(0, MAX_IDS_IN_MESSAGE) : failedEvents;
    String more = failedEvents.size() > MAX_IDS_IN_MESSAGE ? " ..." : "";
    Exception failure = new Exception("Failed to send one or more events (" + failedEvents.size() + "): "
        + shown + more, firstFailure);
    failedEvents.clear();
    firstFailure = null;
    return failure;
  }
}
