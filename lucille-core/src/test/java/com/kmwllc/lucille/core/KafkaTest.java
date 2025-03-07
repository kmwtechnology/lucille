package com.kmwllc.lucille.core;

import com.kmwllc.lucille.message.KafkaUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KafkaTest {

  @Rule
  public EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, false, 1).kafkaPorts(9090).zkPort(9091);

  KafkaTemplate<String, String> template;

  @Before
  public void setUp() {
    Map<String, Object> producerProps =
        KafkaTestUtils.producerProps(embeddedKafka.getEmbeddedKafka());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    ProducerFactory<String, String> pf =
        new DefaultKafkaProducerFactory<>(producerProps);
    template = new KafkaTemplate<>(pf);

    Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("lucille_workers", "false", embeddedKafka.getEmbeddedKafka());
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
    template.setConsumerFactory(cf);
  }

  @Test
  public void testRunInKafkaLocalMode() throws Exception {
    // run two connectors in "Kafka local mode"
    // in "kafka local mode" Workers, Indexers, the Connector/Publisher will run in separate threads,
    // but they will communicate via Kafka (here we use an embedded kafka cluster)
    Config config = ConfigFactory.load("KafkaTest/twoConnectors.conf");
    RunResult result =
        Runner.run(config, Runner.RunType.KAFKA_LOCAL);
    assertTrue(result.getStatus());

    // connector1 will feed 3 documents to pipeline1, so there should be 3 messages in each of
    // the source, dest, and event topics after the run is complete
    String pipeline1SourceTopicName = KafkaUtils.getSourceTopicName("pipeline1", ConfigFactory.empty());
    String pipeline1DestTopicName = KafkaUtils.getDestTopicName("pipeline1");
    String pipeline1EventTopicName = KafkaUtils.getEventTopicName(ConfigFactory.empty(), "pipeline1", result.getRunId());

    // check there are three entries in source + dest

    // Trying to get a record that doesn't exist won't return any results. (we always request one extra record
    // that we won't get / don't expect to be there to ensure the exact amount we want are actually in Kafka.)
    ConsumerRecords<String, String> sourceRecords = template.receive(List.of(
        new TopicPartitionOffset(pipeline1SourceTopicName, 0, 0L),
        new TopicPartitionOffset(pipeline1SourceTopicName, 0, 1L),
        new TopicPartitionOffset(pipeline1SourceTopicName, 0, 2L),
        new TopicPartitionOffset(pipeline1EventTopicName, 0, 3L)
    ));

    assertEquals(3, sourceRecords.count());

    ConsumerRecords<String, String> destRecords = template.receive(List.of(
        new TopicPartitionOffset(pipeline1DestTopicName, 0, 0L),
        new TopicPartitionOffset(pipeline1DestTopicName, 0, 1L),
        new TopicPartitionOffset(pipeline1DestTopicName, 0, 2L),
        new TopicPartitionOffset(pipeline1EventTopicName, 0, 3L)
    ));

    assertEquals(3, destRecords.count());

    // check three entries in event and also run the JSON on it...
    ConsumerRecords<String, String> eventRecords = template.receive(List.of(
        new TopicPartitionOffset(pipeline1EventTopicName, 0, 0L),
        new TopicPartitionOffset(pipeline1EventTopicName, 0, 1L),
        new TopicPartitionOffset(pipeline1EventTopicName, 0, 2L),
        new TopicPartitionOffset(pipeline1EventTopicName, 0, 3L)
    ));

    assertEquals(3, eventRecords.count());

    List<ConsumerRecord<String, String>> eventRecordsList = eventRecords.records(new TopicPartition(pipeline1EventTopicName, 0));

    Event event0 = Event.fromJsonString(eventRecordsList.get(0).value());
    Event event1 = Event.fromJsonString(eventRecordsList.get(1).value());
    Event event2 = Event.fromJsonString(eventRecordsList.get(2).value());
    // in local kafka mode (or fully distributed kafka mode) as opposed to hybrid mode,
    // completion events will contain kafka metadata taken from messages found on the destination topic,
    // not the source topic, because that's where the indexer consumes from
    assertEquals(KafkaUtils.getDestTopicName("pipeline1"), event0.getTopic());
    assertEquals(KafkaUtils.getDestTopicName("pipeline1"), event1.getTopic());
    assertEquals(KafkaUtils.getDestTopicName("pipeline1"), event2.getTopic());
    assertEquals(Integer.valueOf(0), event1.getPartition());
    assertEquals(Long.valueOf(1), event1.getOffset());
    assertEquals(Event.Type.FINISH, event1.getType());

    // connector2 will feed 1 documents to pipeline1, so there should be 1 messages in each of
    // the source, dest, and event topics after the run is complete
    String pipeline2SourceTopicName = KafkaUtils.getSourceTopicName("pipeline2", ConfigFactory.empty());
    sourceRecords = template.receive(List.of(
        new TopicPartitionOffset(pipeline2SourceTopicName, 0, 0L),
        new TopicPartitionOffset(pipeline2SourceTopicName, 0, 1L)
    ));
    assertEquals(1, sourceRecords.count());

    String pipeline2DestTopicName = KafkaUtils.getDestTopicName("pipeline2");
    destRecords = template.receive(List.of(
        new TopicPartitionOffset(pipeline2DestTopicName, 0, 0L),
        new TopicPartitionOffset(pipeline2DestTopicName, 0, 1L)
    ));
    assertEquals(1, destRecords.count());

    List<ConsumerRecord<String, String>> destRecordsList = destRecords.records(new TopicPartition(pipeline2DestTopicName, 0));

    // verify that the document was properly written to the destination topic
    System.out.println(destRecordsList.get(0));
    System.out.println(destRecordsList.get(0).value());
    Document doc = Document.createFromJson(destRecordsList.get(0).value());
    assertEquals("2", doc.getId());
    assertEquals("apple", doc.getString("field1"));

    String pipeline2EventTopicName = KafkaUtils.getEventTopicName(ConfigFactory.empty(), "pipeline2", result.getRunId());
    eventRecords = template.receive(List.of(
        new TopicPartitionOffset(pipeline2EventTopicName, 0, 0L),
        new TopicPartitionOffset(pipeline2EventTopicName, 0, 1L)
    ));
    assertEquals(1, eventRecords.count());
  }

}
