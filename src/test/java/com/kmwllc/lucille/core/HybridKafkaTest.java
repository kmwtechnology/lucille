package com.kmwllc.lucille.core;

import com.kmwllc.lucille.message.KafkaUtils;
import com.kmwllc.lucille.message.LocalMessageManager;
import com.kmwllc.lucille.util.RecordingLinkedBlockingQueue;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.SendKeyValues;
import net.mguenther.kafka.junit.TopicConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;

@RunWith(JUnit4.class)
public class HybridKafkaTest {

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

    String pipelineName = "pipeline1";
    String sourceTopic = KafkaUtils.getSourceTopicName(pipelineName);
    kafka.createTopic(TopicConfig.withName(sourceTopic));

    List<KeyValue<String, String>> records = new ArrayList<>();

    Document doc1 = new Document("doc1");
    records.add(new KeyValue<>("doc1", doc1.toString()));

    kafka.send(SendKeyValues.to(sourceTopic, records));

    WorkerIndexer workerIndexer = new WorkerIndexer();
    Config config = ConfigFactory.load("HybridKafkaTest/config.conf");

    RecordingLinkedBlockingQueue<KafkaDocument> pipelineDest =
      new RecordingLinkedBlockingQueue<>();

    RecordingLinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets =
      new RecordingLinkedBlockingQueue<>();


    workerIndexer.start(config, "pipeline1", pipelineDest, offsets);

    Thread.sleep(6000);

    workerIndexer.stop();

    assertEquals(0, pipelineDest.size());
    assertEquals(0, offsets.size());

    assertEquals(1, pipelineDest.getHistory().size());
    assertEquals(1, offsets.getHistory().size());

  }
}