package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.KafkaDocument;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class HybridIndexerMessageManager implements IndexerMessageManager {

  private final LinkedBlockingQueue<Document> pipelineDest;
  private final LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets;

  // a counter that can be shared among all indexers running in a JVM
  // to provide a mechanism for determining when a certain number of documents have been
  // indexed, even though we can't predict how many documents any particular indexer instance
  // will handle
  private final AtomicLong indexEventCounter;

  public HybridIndexerMessageManager(LinkedBlockingQueue<Document> pipelineDest,
                                     LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets) {
    this.pipelineDest = pipelineDest;
    this.offsets = offsets;
    this.indexEventCounter = null;
  }

  public HybridIndexerMessageManager(LinkedBlockingQueue<Document> pipelineDest,
                                     LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets,
                                     AtomicLong counter) {
    this.pipelineDest = pipelineDest;
    this.offsets = offsets;
    this.indexEventCounter = counter;
  }

  @Override
  public Document pollCompleted() throws Exception {
    return pipelineDest.poll(LocalMessageManager.POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  @Override
  public void sendEvent(Event event) throws Exception {
    // events are not sent anywhere in hybrid mode, but may be counted
    if (indexEventCounter != null) {
      indexEventCounter.getAndIncrement();
    }
  }

  @Override
  public void sendEvent(Document document, String message, Event.Type type) throws Exception {
    // events are not sent anywhere in hybrid mode, but may be counted
    if (indexEventCounter != null) {
      indexEventCounter.getAndIncrement();
    }
  }

  @Override
  public void close() throws Exception {
  }

  // TODO document assumptions about ordering of documents in batch
  @Override
  public void batchComplete(List<Document> batch) throws InterruptedException {
    if (batch.isEmpty()) {
      return;
    }
    Map<TopicPartition,OffsetAndMetadata> batchOffsets = new HashMap<>();
    for (Document doc : batch) {

      if (!(doc instanceof KafkaDocument)) {
        continue;
      }

      KafkaDocument kDoc = (KafkaDocument)doc;
      TopicPartition topicPartition = new TopicPartition(kDoc.getTopic(), kDoc.getPartition());
      // per the kafka docs:
      // "Note: The committed offset should always be the offset of the next message that your application will read.
      // Thus, when calling commitSync(offsets) you should add one to the offset of the last message processed."
      OffsetAndMetadata offset = new OffsetAndMetadata(kDoc.getOffset()+1);
      batchOffsets.put(topicPartition,offset);
    }
    if (!batchOffsets.isEmpty()) {
      offsets.put(batchOffsets);
    }
  }
}
