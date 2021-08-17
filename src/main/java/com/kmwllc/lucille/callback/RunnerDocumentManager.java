package com.kmwllc.lucille.callback;

import com.kmwllc.lucille.core.ConfigAccessor;
import com.typesafe.config.Config;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class RunnerDocumentManager {

  public static final Logger log = LoggerFactory.getLogger(RunnerDocumentManager.class);
  private final Config config = ConfigAccessor.loadConfig();

  private final KafkaProducer<String, String> kafkaProducer;
  private final Consumer<String, String> confirmationConsumer;
  private final String runId;
  private final Admin kafkaAdminClient;

  public RunnerDocumentManager(String runId) {
    this.runId = runId;
    this.kafkaProducer = KafkaUtils.createProducer();
    Properties consumerProps = KafkaUtils.createConsumerProps();
    consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "lucille-3");
    // TODO: create confirmation topic explicitly instead of relying on auto-create; delete topic when finished
    this.confirmationConsumer = new KafkaConsumer(consumerProps);
    this.confirmationConsumer.subscribe(Collections.singletonList(KafkaUtils.getEventTopicName(runId)));
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrapServers"));
    this.kafkaAdminClient = Admin.create(props);
  }

  public Event retrieveConfirmation() throws Exception {
    ConsumerRecords<String, String> consumerRecords = confirmationConsumer.poll(KafkaUtils.POLL_INTERVAL);
    if (consumerRecords.count() > 0) {
      confirmationConsumer.commitSync();
      log.info("FOUND RECEIPT");
      ConsumerRecord<String, String> record = consumerRecords.iterator().next();
      return Event.fromJsonString(record.value());
    }
    return null;
  }

  public boolean isEventTopicEmpty(String runId) throws Exception {
    return getLag(KafkaUtils.getEventTopicName(runId))==0;
  }

  private int getLag(String topic) throws Exception {

    Map<String, Long> map = new HashMap<>();

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


  public void close() throws Exception {
    confirmationConsumer.close();
    kafkaProducer.close();
  }


}
