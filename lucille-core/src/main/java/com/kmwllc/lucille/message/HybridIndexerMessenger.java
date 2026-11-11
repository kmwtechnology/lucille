package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.EventFieldConfig;
import com.typesafe.config.Config;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An IndexerMessenger for hybrid mode: takes documents from the in-memory queue a co-located Worker fills, and reports
 * completed batches, and batches it did not send, to that Worker's {@link HybridWorkerMessenger}, which owns the Kafka
 * consumer and commits its offsets. Ownership checks before each send are answered there too.
 *
 * <p> Internal to Lucille: {@link com.kmwllc.lucille.core.WorkerIndexer} creates it, sharing its queues with a
 * {@link HybridWorkerMessenger}. It is public only because the two are in different packages.
 */
public class HybridIndexerMessenger implements IndexerMessenger {

  private static final Logger log = LoggerFactory.getLogger(HybridIndexerMessenger.class);

  private final BlockingQueue<Document> pipelineDest;
  private final HybridWorkerMessenger workerMessenger;
  private final KafkaProducer<String, String> kafkaEventProducer;
  // only created when the fail topic is enabled, so an indexer that doesn't use it needs no document producer config
  private final KafkaProducer<String, Document> failTopicProducer;
  private final String pipelineName;
  private final Config config;
  private final EventFieldConfig eventFieldConfig;

  // a thread-safe Set that can be shared among all indexers running in a JVM
  // to provide a mechanism for determining when a certain number of unique documents have been
  // indexed, even though we can't predict how many documents any particular indexer instance
  // will handle, and we can't know whether a document will be indexed more than once
  // (reprocessing can happen when there's a kafka consumer group rebalance that happens
  // before all offsets have been committed)
  private final Set<String> idSet;

  public HybridIndexerMessenger(Config config,
      BlockingQueue<Document> pipelineDest,
      HybridWorkerMessenger workerMessenger,
      Set<String> idSet,
      String pipelineName) {
    this.pipelineDest = pipelineDest;
    this.workerMessenger = workerMessenger;
    this.idSet = idSet;
    this.kafkaEventProducer = KafkaUtils.createEventProducer(config);
    this.failTopicProducer = KafkaUtils.indexerFailTopicEnabled(config) ? KafkaUtils.createDocumentProducer(config) : null;
    this.pipelineName = pipelineName;
    this.config = config;
    this.eventFieldConfig = EventFieldConfig.fromConfig(config);
  }

  @Override
  public Document pollDocToIndex() throws MessengerException {
    try {
      return pipelineDest.poll(LocalMessenger.POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new MessengerException("Interrupted while polling for a document to index", e);
    }
  }

  @Override
  public void sendEvent(Event event) throws MessengerException {
    if (idSet != null) {
      idSet.add(event.getDocumentId());
    }
    if (kafkaEventProducer != null) {
      String confirmationTopicName = KafkaUtils.getEventTopicName(config, pipelineName, event.getRunId());
      try {
        kafkaEventProducer.send(new ProducerRecord<>(confirmationTopicName, event.getDocumentId(), event.toString())).get();
      } catch (ExecutionException e) {
        throw KafkaUtils.toMessengerException("Sending " + event.getType() + " event for document " + event.getDocumentId(), e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new MessengerException("Sending " + event.getType() + " event for document " + event.getDocumentId() + " was interrupted", e);
      }
    }
  }

  @Override
  public void sendEvent(Document document, String message, Event.Type type) throws MessengerException {
    if (idSet != null) {
      idSet.add(document.getId());
    }
    if (kafkaEventProducer != null) {
      Event event = new Event(document, message, type, eventFieldConfig);
      sendEvent(event);
    }
  }

  @Override
  public void sendEvents(List<Document> documents, String message, Event.Type type) throws MessengerException {
    if (documents.isEmpty()) {
      return;
    }

    if (idSet != null) {
      for (Document document : documents) {
        idSet.add(document.getId());
      }
    }

    if (kafkaEventProducer == null) {
      return;
    }

    AtomicReference<Exception> sendException = new AtomicReference<>();

    for (Document document : documents) {
      Event event = new Event(document, message, type, eventFieldConfig);
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
      throw new MessengerException("Failed to send one or more events", e);
    }
  }

  @Override
  public void sendFailed(Document document) throws MessengerException {
    KafkaUtils.sendToFailTopic(failTopicProducer, pipelineName, document);
  }

  @Override
  public void close() {
    if (kafkaEventProducer != null) {
      kafkaEventProducer.close();
    }
    if (failTopicProducer != null) {
      failTopicProducer.close();
    }
  }

  @Override
  public void batchComplete(List<Document> batch) {
    workerMessenger.batchCompleted(batch);
  }

  @Override
  public boolean ownsInput(Document doc) {
    return workerMessenger.ownsInput(doc);
  }

  @Override
  public void batchNotSent(List<Document> batch) {
    workerMessenger.batchNotSent(batch);
  }

  // Reports batches on completion (not at poll), so it is safe for concurrent batches.
  @Override
  public boolean commitsOnBatchCompletion() {
    return true;
  }
}
