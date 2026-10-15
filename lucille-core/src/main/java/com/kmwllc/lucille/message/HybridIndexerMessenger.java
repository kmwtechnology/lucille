package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.KafkaDocument;
import com.typesafe.config.Config;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HybridIndexerMessenger implements IndexerMessenger {

  private static final Logger log = LoggerFactory.getLogger(HybridIndexerMessenger.class);

  private final LinkedBlockingQueue<Document> pipelineDest;
  private final LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets;
  private final KafkaEventSender eventSender;
  private final String pipelineName;
  private final Config config;

  // a thread-safe Set that can be shared among all indexers running in a JVM
  // to provide a mechanism for determining when a certain number of unique documents have been
  // indexed, even though we can't predict how many documents any particular indexer instance
  // will handle, and we can't know whether a document will be indexed more than once
  // (reprocessing can happen when there's a kafka consumer group rebalance that happens
  // before all offsets have been committed)
  private final Set idSet;

  public HybridIndexerMessenger(Config config,
      LinkedBlockingQueue<Document> pipelineDest,
      LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets,
      Set<String> idSet,
      String pipelineName) {
    this.pipelineDest = pipelineDest;
    this.offsets = offsets;
    this.idSet = idSet;
    this.eventSender = new KafkaEventSender(config, pipelineName);
    this.pipelineName = pipelineName;
    this.config = config;
  }

  @Override
  public Document pollDocToIndex() throws Exception {
    return pipelineDest.poll(LocalMessenger.POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  /**
   * Sends the Event without waiting for it to be acknowledged. It is delivered by the next sendEvents() or
   * batchComplete() call.
   */
  @Override
  public void sendEvent(Event event) throws Exception {
    if (idSet != null) {
      idSet.add(event.getDocumentId());
    }
    eventSender.send(event);
  }

  @Override
  public void sendEvent(Document document, String message, Event.Type type) throws Exception {
    if (idSet != null) {
      idSet.add(document.getId());
    }
    if (eventSender.isEnabled()) {
      eventSender.send(new Event(document, message, type));
    }
  }

  @Override
  public void sendEvents(List<Document> documents, String message, Event.Type type) throws Exception {
    if (documents.isEmpty()) {
      return;
    }

    if (idSet != null) {
      for (Document document : documents) {
        idSet.add(document.getId());
      }
    }

    if (!eventSender.isEnabled()) {
      return;
    }

    for (Document document : documents) {
      eventSender.send(new Event(document, message, type));
    }
    eventSender.flush();
  }

  @Override
  public void close() throws Exception {
    eventSender.close();
  }

  // TODO document assumptions about ordering of documents in batch
  @Override
  public void batchComplete(List<Document> batch) throws Exception {
    // Deliver the batch's events (FAIL events are sent one at a time) before its offsets are queued for commit, so a
    // committed offset never covers a document whose terminal event is still in the producer. As when sends were
    // synchronous, a failed event send is reported but does not hold back the offsets.
    Exception flushFailure = null;
    try {
      eventSender.flush();
    } catch (Exception e) {
      flushFailure = e;
    }
    try {
      queueOffsets(batch);
    } catch (InterruptedException e) {
      if (flushFailure != null) {
        e.addSuppressed(flushFailure);
      }
      throw e;
    }
    if (flushFailure != null) {
      throw flushFailure;
    }
  }

  private void queueOffsets(List<Document> batch) throws InterruptedException {
    if (batch.isEmpty()) {
      return;
    }
    Map<TopicPartition, OffsetAndMetadata> batchOffsets = new HashMap<>();
    for (Document doc : batch) {

      if (!(doc instanceof KafkaDocument)) {
        continue;
      }

      KafkaDocument kDoc = (KafkaDocument) doc;
      TopicPartition topicPartition = new TopicPartition(kDoc.getTopic(), kDoc.getPartition());
      // per the kafka docs:
      // "Note: The committed offset should always be the offset of the next message that your application will read.
      // Thus, when calling commitSync(offsets) you should add one to the offset of the last message processed."
      OffsetAndMetadata offset = new OffsetAndMetadata(kDoc.getOffset() + 1);
      batchOffsets.put(topicPartition, offset);
    }
    if (!batchOffsets.isEmpty()) {
      offsets.put(batchOffsets);
    }
  }
}
