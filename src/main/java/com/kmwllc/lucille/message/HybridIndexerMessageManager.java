package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.KafkaDocument;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


public class HybridIndexerMessageManager implements IndexerMessageManager {

  private final LinkedBlockingQueue<Document> pipelineDest;
  private final LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets;

  // a thread-safe Set that can be shared among all indexers running in a JVM
  // to provide a mechanism for determining when a certain number of unique documents have been
  // indexed, even though we can't predict how many documents any particular indexer instance
  // will handle, and we can't know whether a document will be indexed more than once
  // (reprocessing can happen when there's a kafka consumer group rebalance that happens
  // before all offsets have been committed)
  private final Set idSet;

  public HybridIndexerMessageManager(LinkedBlockingQueue<Document> pipelineDest,
                                     LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets) {
    this.pipelineDest = pipelineDest;
    this.offsets = offsets;
    this.idSet = null;
  }

  public HybridIndexerMessageManager(LinkedBlockingQueue<Document> pipelineDest,
                                     LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets,
                                     Set<String> idSet) {
    this.pipelineDest = pipelineDest;
    this.offsets = offsets;
    this.idSet = idSet;
  }

  @Override
  public Document pollCompleted() throws Exception {
    return pipelineDest.poll(LocalMessageManager.POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  @Override
  public void sendEvent(Event event) throws Exception {
    // events are not sent anywhere in hybrid mode, but may be counted
    if (idSet != null) {
      idSet.add(event.getDocumentId());
    }
  }

  @Override
  public void sendEvent(Document document, String message, Event.Type type) throws Exception {
    // events are not sent anywhere in hybrid mode, but may be counted
    if (idSet != null) {
      idSet.add(document.getId());
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
