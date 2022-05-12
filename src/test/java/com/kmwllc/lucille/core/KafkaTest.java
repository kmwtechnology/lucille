package com.kmwllc.lucille.core;

import com.kmwllc.lucille.message.KafkaUtils;
import com.typesafe.config.ConfigFactory;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ReadKeyValues;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class KafkaTest {

  private EmbeddedKafkaCluster kafka;

  @Before
  public void setupKafka() {
    kafka = provisionWith(defaultClusterConfig());
    kafka.start();
  }

  @After
  public void tearDownKafka() {
    kafka.stop();
  }

  @Test
  public void testRunInKafkaLocalMode() throws Exception {

    // run two connectors in "Kafka local mode"
    // in "kafka local mode" Workers, Indexers, the Connector/Publisher will run in separate threads,
    // but they will communicate via Kafka (here we use an embedded kafka cluster)
    RunResult result =
      Runner.run(ConfigFactory.load("KafkaTest/twoConnectors.conf"), Runner.RunType.KAFKA_LOCAL);
    assertTrue(result.getStatus());

    // connector1 will feed 3 documents to pipeline1, so there should be 3 messages in each of
    // the source, dest, and event topics after the run is complete
    assertEquals(3, kafka.read(ReadKeyValues
      .from(KafkaUtils.getSourceTopicName("pipeline1", ConfigFactory.empty()))
      .seekTo(0, 0)).size());

    assertEquals(3, kafka.read(ReadKeyValues
      .from(KafkaUtils.getDestTopicName("pipeline1"))
      .seekTo(0, 0)).size());

    assertEquals(3, kafka.read(ReadKeyValues
      .from(KafkaUtils.getEventTopicName("pipeline1", result.getRunId()))
      .seekTo(0, 0)).size());

    // connector2 will feed 1 documents to pipeline1, so there should be 1 messages in each of
    // the source, dest, and event topics after the run is complete
    assertEquals(1, kafka.read(ReadKeyValues
      .from(KafkaUtils.getSourceTopicName("pipeline2", ConfigFactory.empty()))
      .seekTo(0, 0)).size());

    List<KeyValue<String, String>> records = kafka.read(ReadKeyValues
      .from(KafkaUtils.getDestTopicName("pipeline2"))
      .seekTo(0, 0));
    assertEquals(1, records.size());

    // verify that the document was properly written to the destination topic
    Document doc = Document.fromJsonString(records.get(0).getValue());
    assertEquals("2", doc.getId());
    assertEquals("apple", doc.getString("field1"));

    assertEquals(1, kafka.read(ReadKeyValues
      .from(KafkaUtils.getEventTopicName("pipeline2", result.getRunId()))
      .seekTo(0, 0)).size());
  }

}
