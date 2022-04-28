package com.kmwllc.lucille.core;

import com.kmwllc.lucille.util.RecordingLinkedBlockingQueue;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.SendKeyValues;
import net.mguenther.kafka.junit.TopicConfig;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
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
import java.util.Properties;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static org.junit.Assert.assertEquals;

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
  public void testRunInKafkaHybridMode() throws Exception {
    Config config = ConfigFactory.load("HybridKafkaTest/config.conf");

    String sourceTopic = config.getString("kafka.sourceTopic");
    kafka.createTopic(TopicConfig.withName(sourceTopic));

    sendDoc("doc1", sourceTopic);

    WorkerIndexer workerIndexer = new WorkerIndexer();

    RecordingLinkedBlockingQueue<KafkaDocument> pipelineDest =
      new RecordingLinkedBlockingQueue<>();

    RecordingLinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets =
      new RecordingLinkedBlockingQueue<>();

    workerIndexer.start(config, "pipeline1", pipelineDest, offsets, true);

    sendDoc("doc2", sourceTopic);
    sendDoc("doc3", sourceTopic);

    Thread.sleep(6000);

    workerIndexer.stop();

    // confirm that the consumer group is looking at offset 3 in the source topic,
    // indicating it has consumed 3 documents (at offsets 0, 1, 2) and that offsets
    // have been properly committed
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrapServers"));
    Admin kafkaAdminClient = Admin.create(props);
    Map<TopicPartition, OffsetAndMetadata> retrievedOffsets =
      kafkaAdminClient.listConsumerGroupOffsets(config.getString("kafka.consumerGroupId"))
        .partitionsToOffsetAndMetadata().get();
    TopicPartition sourceTopicPartition = new TopicPartition(sourceTopic,0);
    assertEquals(3, retrievedOffsets.get(sourceTopicPartition).offset());

    // pipelineDest and offset queues should have been drained
    assertEquals(0, pipelineDest.size());
    assertEquals(0, offsets.size());

    // three docs should have been added to piptlineDest queue
    assertEquals(3, pipelineDest.getHistory().size());

    // one offset map should have been added to offset queue, containing offset 3 for partition 0
    assertEquals(1, offsets.getHistory().size());
    assertEquals(1, offsets.getHistory().get(0).entrySet().size());
    assertEquals(3, offsets.getHistory().get(0).get(sourceTopicPartition).offset());
  }

  private Document sendDoc(String id, String topic) throws Exception {
    List<KeyValue<String, String>> records = new ArrayList<>();
    Document doc = new Document(id);
    records.add(new KeyValue<>(id, doc.toString()));
    kafka.send(SendKeyValues.to(topic, records));
    return doc;
  }
}