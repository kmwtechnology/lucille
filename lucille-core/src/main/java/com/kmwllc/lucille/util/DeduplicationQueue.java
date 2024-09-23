package com.kmwllc.lucille.util;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.KafkaDocument;
import com.typesafe.config.ConfigException.Null;
import dev.langchain4j.agent.tool.P;
import io.swagger.v3.oas.annotations.links.Link;
import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import kotlin.experimental.ExperimentalTypeInference;
import net.jodah.expiringmap.ExpiringMap;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class DeduplicationQueue {

  private final Logger log = LoggerFactory.getLogger(DeduplicationQueue.class);

  private ExpiringMap<String, KafkaDocument> expiryMap;
  private Queue<KafkaDocument> expiredDocuments;
  private LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets;

  private static DeduplicationQueue instance;

  private DeduplicationQueue(
      ExpiringMap<String, KafkaDocument> expiryMap,
      Queue<KafkaDocument> expiredDocuments,
      LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets) {
    this.expiryMap = expiryMap;
    this.expiredDocuments = expiredDocuments;
    this.offsets = offsets;
  }

  public synchronized static DeduplicationQueue getInstance() {
    if (instance == null) {
      throw new RuntimeException("DeduplicationQueue has not been initialized");
    }

    return instance;
  }

  public synchronized static void initialize(int timeout, LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets) {
    Queue<KafkaDocument> expiredDocuments = new LinkedBlockingQueue<>();
    ExpiringMap<String, KafkaDocument> expiryMap = ExpiringMap.builder()
        .expiration(timeout, TimeUnit.SECONDS)
        .expirationListener((String key, KafkaDocument doc) -> expiredDocuments.add(doc))
        .build();

    instance = new DeduplicationQueue(expiryMap, expiredDocuments, offsets);
  }

  public synchronized void addToExpiryQueue(KafkaDocument doc) throws InterruptedException, NullPointerException {
    if (expiryMap.containsKey(doc.getId())) {
      KafkaDocument replacedDoc = expiryMap.replace(doc.getId(), doc);
      TopicPartition replacedPartition = new TopicPartition(replacedDoc.getTopic(), replacedDoc.getPartition());

      // per the kafka docs:
      // "Note: The committed offset should always be the offset of the next message that your application will read.
      // Thus, when calling commitSync(offsets) you should add one to the offset of the last message processed."
      OffsetAndMetadata offset = new OffsetAndMetadata(replacedDoc.getOffset() + 1);
      offsets.put(Collections.singletonMap(replacedPartition, offset));
    } else {
      expiryMap.put(doc.getId(), doc);
    }
  }

  public synchronized KafkaDocument pollExpiredDocuments() {
    if (expiredDocuments.isEmpty()) {
      return null;
    } else {
      return expiredDocuments.poll();
    }
  }

  public synchronized LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> getOffsets() {
    return offsets;
  }
}
