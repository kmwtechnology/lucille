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

public class HybridIndexerMessageManager implements IndexerMessageManager {

  private final LinkedBlockingQueue<Document> pipelineDest;
  private final LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets;

  public HybridIndexerMessageManager(LinkedBlockingQueue<Document> pipelineDest,
                                     LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets) {
    this.pipelineDest = pipelineDest;
    this.offsets = offsets;
  }

  @Override
  public Document pollCompleted() throws Exception {
    return pipelineDest.poll(LocalMessageManager.POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  @Override
  public void sendEvent(Event event) throws Exception {
    // no-op -- Events are not tracked in hybrid mode
  }

  @Override
  public void sendEvent(Document document, String message, Event.Type type) throws Exception {
    // no-op -- Events are not tracked in hybrid mode
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
