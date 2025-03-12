package com.kmwllc.lucille.core;

import com.kmwllc.lucille.message.KafkaUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KafkaTest {

  // This is a class-level Embedded instance of Kafka. If, in the future, additional tests are added here,
  // it is important that they use unique topic names to avoid conflicts.
  @ClassRule
  public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, false, 1).kafkaPorts(9090).zkPort(9091);

  private Consumer<String, String> consumer;

  @Before
  public void setUp() {
    Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("lucille_workersss", "false", embeddedKafka.getEmbeddedKafka());
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
    consumer = cf.createConsumer();
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

    // Using our own consumer to subscribe to the topics and get the records.
    consumer.subscribe(List.of(pipeline1SourceTopicName, pipeline1DestTopicName, pipeline1EventTopicName));
    ConsumerRecords<String, String> pipeline1Records = KafkaTestUtils.getRecords(consumer);

    List<ConsumerRecord<String, String>> pipeline1SourceRecords = pipeline1Records.records(new TopicPartition(pipeline1SourceTopicName, 0));
    assertEquals(3, pipeline1SourceRecords.size());

    List<ConsumerRecord<String, String>> pipeline1DestRecords = pipeline1Records.records(new TopicPartition(pipeline1DestTopicName, 0));
    assertEquals(3, pipeline1DestRecords.size());

    List<ConsumerRecord<String, String>> pipeline1EventRecords = pipeline1Records.records(new TopicPartition(pipeline1EventTopicName, 0));
    assertEquals(3, pipeline1EventRecords.size());

    Event event0 = Event.fromJsonString(pipeline1EventRecords.get(0).value());
    Event event1 = Event.fromJsonString(pipeline1EventRecords.get(1).value());
    Event event2 = Event.fromJsonString(pipeline1EventRecords.get(2).value());
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
    String pipeline2DestTopicName = KafkaUtils.getDestTopicName("pipeline2");
    String pipeline2EventTopicName = KafkaUtils.getEventTopicName(ConfigFactory.empty(), "pipeline2", result.getRunId());

    consumer.subscribe(List.of(pipeline2SourceTopicName, pipeline2DestTopicName, pipeline2EventTopicName));
    ConsumerRecords<String, String> pipeline2Records = KafkaTestUtils.getRecords(consumer);

    List<ConsumerRecord<String, String>> pipeline2SourceRecords = pipeline2Records.records(new TopicPartition(pipeline2SourceTopicName, 0));
    assertEquals(1, pipeline2SourceRecords.size());

    List<ConsumerRecord<String, String>> pipeline2DestRecords = pipeline2Records.records(new TopicPartition(pipeline2DestTopicName, 0));
    assertEquals(1, pipeline2DestRecords.size());

    List<ConsumerRecord<String, String>> pipeline2EventRecords = pipeline2Records.records(new TopicPartition(pipeline2EventTopicName, 0));
    assertEquals(1, pipeline2EventRecords.size());

    // verify that the document was properly written to the destination topic
    Document doc = Document.createFromJson(pipeline2DestRecords.get(0).value());
    assertEquals("2", doc.getId());
    assertEquals("apple", doc.getString("field1"));
  }

}
