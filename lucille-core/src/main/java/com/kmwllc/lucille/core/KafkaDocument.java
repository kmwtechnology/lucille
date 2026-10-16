package com.kmwllc.lucille.core;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Objects;

/**
 * A Document that is retrieved from a Kafka topic and may therefore possess a Kafka topic name, partition, offset, and key
 * along with typical Document field data.
 *
 * In Lucille, plain (non-Kafka) Documents are written to Kafka. Those same Documents are then retrieved/deserialized
 * from kafka as KafkaDocuments. The topic, partition, offset, and key is copied from the Kafka ConsumerRecord onto the
 * KafkaDocument after deserialization.
 */
public class KafkaDocument extends JsonDocument {

  private String topic;
  private int partition;
  private long offset;
  private String key;
  // The partition-assignment generation this document was delivered under, stamped by the indexer messenger at poll
  // time so that offset commits can reject a document whose partition was revoked and reassigned since it was polled.
  // -1 means "not stamped" (documents outside the concurrent-indexer path never set it).
  private long deliveryGeneration = -1;

  public KafkaDocument(ObjectNode data) throws DocumentException {
    super(data);
  }

  public void setKafkaMetadata(ConsumerRecord<String, ?> record) {
    this.topic = record.topic();
    this.partition = record.partition();
    this.offset = record.offset();
    this.key = record.key();
  }

  public KafkaDocument(ConsumerRecord<String, String> record) throws Exception {
    super((ObjectNode) MAPPER.readTree(record.value()));
    setKafkaMetadata(record);
  }

  KafkaDocument(ObjectNode data, String topic, int partition, long offset, String key) throws DocumentException {
    super(data);
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
    this.key = key;
  }

  /**
   * Creates a deep copy of the given Document, now as a KafkaDocument with kafka metadata.
   */
  KafkaDocument(Document doc, String topic, int partition, long offset, String key) throws DocumentException {
    super(doc);
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
    this.key = key;
  }


  public String getTopic() {
    return topic;
  }

  public int getPartition() {
    return partition;
  }

  public long getOffset() {
    return offset;
  }

  public String getKey() {
    return key;
  }

  /** The partition-assignment generation this document was delivered under, or -1 if it was never stamped. */
  public long getDeliveryGeneration() {
    return deliveryGeneration;
  }

  /** Stamps the partition-assignment generation this document was delivered under (see field doc). */
  public void setDeliveryGeneration(long deliveryGeneration) {
    this.deliveryGeneration = deliveryGeneration;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other instanceof KafkaDocument) {
      KafkaDocument doc = (KafkaDocument) other;

      return
          Objects.equals(topic, doc.topic) &&
              Objects.equals(partition, doc.partition) &&
              Objects.equals(offset, doc.offset) &&
              Objects.equals(key, doc.key) &&
              data.equals(doc.data);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(data, topic, partition, offset, key);
  }

  @Override
  public KafkaDocument clone() {
    try {
      return new KafkaDocument(data.deepCopy(), topic, partition, offset, key);
    } catch (DocumentException e) {
      throw new IllegalStateException("Document not cloneable", e);
    }
  }
}
