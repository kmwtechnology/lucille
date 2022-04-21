package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;

import com.kmwllc.lucille.core.KafkaDocument;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class HybridIndexerMessageManager implements IndexerMessageManager {

  private final LinkedBlockingQueue<KafkaDocument> pipelineDest;
  private final LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets;

  public HybridIndexerMessageManager(LinkedBlockingQueue<KafkaDocument> pipelineDest,
                                     LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets) {
    this.pipelineDest = pipelineDest;
    this.offsets = offsets;
  }

  @Override
  public KafkaDocument pollCompleted() throws Exception {
    return pipelineDest.poll(LocalMessageManager.POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  @Override
  public void sendEvent(Event event) throws Exception {
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public void batchComplete(List<Document> batch) throws InterruptedException {
    if (batch.isEmpty()) {
      return;
    }
    Map<TopicPartition,OffsetAndMetadata> batchOffsets = new HashMap<>();
    for (Document doc : batch) {
      KafkaDocument kDoc = (KafkaDocument)doc;
      TopicPartition topicPartition = new TopicPartition(kDoc.getTopic(), kDoc.getPartition());
      // per the kafka docs:
      // "Note: The committed offset should always be the offset of the next message that your application will read.
      // Thus, when calling commitSync(offsets) you should add one to the offset of the last message processed."
      OffsetAndMetadata offset = new OffsetAndMetadata(kDoc.getOffset()+1);
      batchOffsets.put(topicPartition,offset);
    }
    offsets.put(batchOffsets);
  }
}
