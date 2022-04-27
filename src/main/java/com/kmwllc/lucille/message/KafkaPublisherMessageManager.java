package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.typesafe.config.Config;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class KafkaPublisherMessageManager implements PublisherMessageManager {

  private final Config config;
  private KafkaProducer<String, Document> kafkaProducer;
  private Consumer<String, String> eventConsumer;
  private String runId;
  private Admin kafkaAdminClient;
  private String pipelineName;

  public KafkaPublisherMessageManager(Config config) {
    this.config = config;
  }

  public void initialize(String runId, String pipelineName) throws Exception {
    if (this.runId!=null) {
      throw new Exception("Already initialized.");
    }
    this.runId = runId;
    this.pipelineName = pipelineName;
    String kafkaClientId = "lucille-publisher-" + pipelineName;
    // TODO: create event topic explicitly instead of relying on auto-create; delete topic when finished
    this.eventConsumer = KafkaUtils.createEventConsumer(config, kafkaClientId);
    this.eventConsumer.subscribe(Collections.singletonList(KafkaUtils.getEventTopicName(pipelineName, runId)));
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrapServers"));
    this.kafkaAdminClient = Admin.create(props);
    this.kafkaProducer = KafkaUtils.createDocumentProducer(config);
  }

  @Override
  public String getRunId() {
    return runId;
  }

  public void sendForProcessing(Document document) throws Exception {
    RecordMetadata result = (RecordMetadata) kafkaProducer.send(
      new ProducerRecord(KafkaUtils.getSourceTopicName(pipelineName), document.getId(), document)).get();
    kafkaProducer.flush();
  }

  /**
   * Polls for an Event that is waiting to be consumed.
   */
  @Override
  public Event pollEvent() throws Exception {
    ConsumerRecords<String, String> consumerRecords = eventConsumer.poll(KafkaUtils.POLL_INTERVAL);
    KafkaUtils.validateAtMostOneRecord(consumerRecords);
    if (consumerRecords.count() > 0) {
      eventConsumer.commitSync();
      ConsumerRecord<String, String> record = consumerRecords.iterator().next();
      return Event.fromJsonString(record.value());
    }
    return null;
  }

  /**
   * Returns true if there are no events waiting to be consumed.
   */
  @Override
  public boolean hasEvents() throws Exception {
    return getLag(KafkaUtils.getEventTopicName(pipelineName, runId))>0;
  }

  private int getLag(String topic) throws Exception {

    Map<TopicPartition, OffsetAndMetadata> offsets =
      kafkaAdminClient.listConsumerGroupOffsets(config.getString("kafka.consumerGroupId"))
        .partitionsToOffsetAndMetadata().get();

    // TODO: throw exception if no offsets found

    Map<TopicPartition, Long> ends = new HashMap<>();
    ends.putAll(kafkaAdminClient.listOffsets(offsets.entrySet().stream().collect(
      Collectors.toMap(Map.Entry::getKey, o -> OffsetSpec.latest()))).all().get()
      .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, o -> o.getValue().offset())));

    int total = 0;
    for (Map.Entry<TopicPartition, OffsetAndMetadata> offset : offsets.entrySet()) {
      if (offset.getKey().topic().equals(topic)) {
//        System.out.println("ADDING: " + offset + " | " + ends.get(offset.getKey()));
        total += (ends.get(offset.getKey()) - offset.getValue().offset());
      }
    }

    return total;
  }

  public void close() {
    kafkaProducer.close();
    eventConsumer.close();
  }

}
