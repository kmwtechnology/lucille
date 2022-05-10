package com.kmwllc.lucille.core;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Objects;

public class KafkaDocument extends Document {

  private String topic;
  private int partition;
  private long offset;
  private String key;

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
    super((ObjectNode)MAPPER.readTree(record.value()));
    setKafkaMetadata(record);
  }

  private KafkaDocument(ObjectNode data, String topic, int partition, long offset, String key) throws DocumentException {
    super(data);
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

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other instanceof KafkaDocument) {
      KafkaDocument doc = (KafkaDocument)other;

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
  public Document clone() {
    try {
      return new KafkaDocument(data.deepCopy(), topic, partition, offset, key);
    } catch (DocumentException e) {
      throw new IllegalStateException("Document not cloneable", e);
    }
  }
}
