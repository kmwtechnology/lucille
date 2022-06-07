package com.kmwllc.lucille.core;

import com.kmwllc.lucille.util.CounterUtils;
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

import java.util.*;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static org.junit.Assert.*;

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
    // use a pipeline that generates children documents to confirm they are handled properly in hybrid mode
    // child docs do not originate in kafka so the logic for committing offsets should ignore them
    Config config = ConfigFactory.load("HybridKafkaTest/childrenConfig.conf");

    String sourceTopic = config.getString("kafka.sourceTopic");
    kafka.createTopic(TopicConfig.withName(sourceTopic).withNumberOfPartitions(1));

    sendDoc("doc1", sourceTopic);

    WorkerIndexer workerIndexer = new WorkerIndexer();

    RecordingLinkedBlockingQueue<Document> pipelineDest =
      new RecordingLinkedBlockingQueue<>();

    RecordingLinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets =
      new RecordingLinkedBlockingQueue<>();

    Set<String> idSet = CounterUtils.getThreadSafeSet();
    workerIndexer.start(config, "pipeline1", pipelineDest, offsets, true, idSet);

    sendDoc("doc2", sourceTopic);
    sendDoc("doc3", sourceTopic);

    CounterUtils.waitUnique(idSet, 3);

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
    assertNotNull(retrievedOffsets.get(sourceTopicPartition));
    assertEquals(3, retrievedOffsets.get(sourceTopicPartition).offset());

    // pipelineDest and offset queues should have been drained
    assertEquals(0, pipelineDest.size());
    assertEquals(0, offsets.size());

    // three docs should have been added to pipelineDest queue
    // each of the three docs we added should have generated two children
    assertEquals(9, pipelineDest.getHistory().size());

    // one offset map should have been added to offset queue, containing offset 3 for partition 0
    assertEquals(1, offsets.getHistory().size());
    assertEquals(1, offsets.getHistory().get(0).entrySet().size());
    assertEquals(3, offsets.getHistory().get(0).get(sourceTopicPartition).offset());
  }

  @Test
  public void testTwoWorkerIndexerPairs() throws Exception {
    Config config = ConfigFactory.load("HybridKafkaTest/noopConfig.conf");

    String sourceTopic = config.getString("kafka.sourceTopic");
    kafka.createTopic(TopicConfig.withName(sourceTopic).withNumberOfPartitions(2));

    for (int i=0;i<500;i++) {
      sendDoc("doc"+i, sourceTopic);
    }

    Set<String> idSet = CounterUtils.getThreadSafeSet();

    RecordingLinkedBlockingQueue<Document> pipelineDest1 =
      new RecordingLinkedBlockingQueue<>();
    RecordingLinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets1 =
      new RecordingLinkedBlockingQueue<>();
    WorkerIndexer workerIndexer1 = new WorkerIndexer();
    workerIndexer1.start(config, "pipeline1",  pipelineDest1, offsets1, true, idSet);

    RecordingLinkedBlockingQueue<Document> pipelineDest2 =
      new RecordingLinkedBlockingQueue<>();
    RecordingLinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets2 =
      new RecordingLinkedBlockingQueue<>();
    WorkerIndexer workerIndexer2 = new WorkerIndexer();
    workerIndexer2.start(config, "pipeline1", pipelineDest2, offsets2, true, idSet);

    for (int i=500;i<1000;i++) {
      sendDoc("doc"+i, sourceTopic);
    }

    CounterUtils.waitUnique(idSet, 1000);

    workerIndexer1.stop();
    workerIndexer2.stop();

    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrapServers"));
    Admin kafkaAdminClient = Admin.create(props);
    Map<TopicPartition, OffsetAndMetadata> retrievedOffsets =
      kafkaAdminClient.listConsumerGroupOffsets(config.getString("kafka.consumerGroupId"))
        .partitionsToOffsetAndMetadata().get();
    TopicPartition partition0 = new TopicPartition(sourceTopic,0);
    TopicPartition partition1 = new TopicPartition(sourceTopic,1);

    // the sum of offsets across the two partitions should be the same as the number of documents
    // consumed. All 1000 documents we added to the source topic should have been consumed.
    assertEquals(1000,
      retrievedOffsets.get(partition0).offset() + retrievedOffsets.get(partition1).offset());

    // we currently have no way to guarantee that each WorkerIndexer
    // received and processed some work
    // assertTrue(pipelineDest1.getHistory().size()>0);
    // assertTrue(pipelineDest2.getHistory().size()>0);

    // total number of docs processed should be >= 1000
    // some docs could have been reprocessed if a partition was reassigned from one
    // worker to another via a rebalance at a time when there were uncommitted offsets;
    // in this case, the new owner of the partition would begin reading from the last committed offset,
    // reprocessing the uncommitted ones
    assertTrue(pipelineDest1.getHistory().size() + pipelineDest2.getHistory().size() >= 1000);

    // make sure each doc we generated is present in at least one of the destination queues,
    // and there are no others
    HashSet<String> idsProcessed = new HashSet<>();
    for (Document d : pipelineDest1.getHistory()) {
      idsProcessed.add(d.getId());
    }
    for (Document d : pipelineDest2.getHistory()) {
      idsProcessed.add(d.getId());
    }
    assertEquals(1000, idsProcessed.size());
    for (int i=0; i<1000; i++) {
      assertTrue(idsProcessed.contains("doc"+i));
    }

  }

  @Test
  public void testCustomDeserializer() throws Exception {
    // documentDeserializer: "com.kmwllc.lucille.core.NonstandardDocumentDeserializer"
    Config config = ConfigFactory.load("HybridKafkaTest/customDeserializer.conf");

    String sourceTopic = config.getString("kafka.sourceTopic");
    kafka.createTopic(TopicConfig.withName(sourceTopic).withNumberOfPartitions(1));

    // inputJson does not have the "id" field required for creating a KafkaDocument
    String inputJson = "{\"myId\":\"doc1\", \"field1\":\"value1\"}";

    // we expect the custom deserializer to copy myId to id
    String outputJson = "{\"myId\":\"doc1\",\"field1\":\"value1\",\"id\":\"doc1\"}";

    List<KeyValue<String, String>> records = new ArrayList<>();
    records.add(new KeyValue<>("doc1", inputJson));
    kafka.send(SendKeyValues.to(sourceTopic, records));

    WorkerIndexer workerIndexer = new WorkerIndexer();

    RecordingLinkedBlockingQueue<Document> pipelineDest =
      new RecordingLinkedBlockingQueue<>();
    RecordingLinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets =
      new RecordingLinkedBlockingQueue<>();

    Set<String> idSet = CounterUtils.getThreadSafeSet();
    workerIndexer.start(config, "pipeline1", pipelineDest, offsets, true, idSet);

    CounterUtils.waitUnique(idSet, 1);

    workerIndexer.stop();

    assertEquals(1, pipelineDest.getHistory().size());
    assertEquals(outputJson, pipelineDest.getHistory().get(0).toString());
  }

  @Test
  public void testWorkerIndexerPool() throws Exception {
    Config config = ConfigFactory.load("HybridKafkaTest/poolConfig.conf");

    String sourceTopic = config.getString("kafka.sourceTopic");
    kafka.createTopic(TopicConfig.withName(sourceTopic).withNumberOfPartitions(5));

    Set<String> idSet = CounterUtils.getThreadSafeSet();
    WorkerIndexerPool pool =
      new WorkerIndexerPool(config,"pipeline1", true, idSet);

    assertEquals(5, pool.getNumWorkers());

    for (int i=0;i<500;i++) {
      sendDoc("doc"+i, sourceTopic);
    }

    pool.start();

    for (int i=500;i<1000;i++) {
      sendDoc("doc"+i, sourceTopic);
    }

    CounterUtils.waitUnique(idSet, 1000);

    pool.stop();

    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrapServers"));
    Admin kafkaAdminClient = Admin.create(props);
    Map<TopicPartition, OffsetAndMetadata> retrievedOffsets =
      kafkaAdminClient.listConsumerGroupOffsets(config.getString("kafka.consumerGroupId"))
        .partitionsToOffsetAndMetadata().get();
    TopicPartition partition0 = new TopicPartition(sourceTopic,0);
    TopicPartition partition1 = new TopicPartition(sourceTopic,1);
    TopicPartition partition2 = new TopicPartition(sourceTopic,2);
    TopicPartition partition3 = new TopicPartition(sourceTopic,3);
    TopicPartition partition4 = new TopicPartition(sourceTopic,4);

    long sumOffsets = retrievedOffsets.get(partition0).offset() +
      retrievedOffsets.get(partition1).offset() +
      retrievedOffsets.get(partition2).offset() +
      retrievedOffsets.get(partition3).offset() +
      retrievedOffsets.get(partition4).offset();

    // the sum of offsets across the two partitions should be the same as the number of documents
    // consumed. All 1000 documents we added to the source topic should have been consumed.
    assertEquals(1000, sumOffsets);

  }


  private Document sendDoc(String id, String topic) throws Exception {
    List<KeyValue<String, String>> records = new ArrayList<>();
    Document doc = new Document(id);
    records.add(new KeyValue<>(id, doc.toString()));
    kafka.send(SendKeyValues.to(topic, records));
    return doc;
  }
}