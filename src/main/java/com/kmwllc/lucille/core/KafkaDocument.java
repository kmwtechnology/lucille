package com.kmwllc.lucille.core;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaDocument extends Document {

  private String topic;
  private int partition;
  private long offset;
  private String key;

  public KafkaDocument(ConsumerRecord<String, String> record) throws Exception {
    super((ObjectNode)MAPPER.readTree(record.value()));
    this.topic = record.topic();
    this.partition = record.partition();
    this.offset = record.offset();
    this.key = record.key();
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
}
