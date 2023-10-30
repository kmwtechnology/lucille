package com.kmwllc.lucille.core;

import com.kmwllc.lucille.message.KafkaUtils;
import com.kmwllc.lucille.util.CounterUtils;
import com.kmwllc.lucille.util.RecordingLinkedBlockingQueue;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ReadKeyValues;
import net.mguenther.kafka.junit.SendKeyValues;
import net.mguenther.kafka.junit.TopicConfig;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class HybridKafkaTest {

  private static final String RUN_ID = "run1";
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

    // 9 docs should be indexed: three parents with two children each
    CounterUtils.waitUnique(idSet, 9);

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
    TopicPartition sourceTopicPartition = new TopicPartition(sourceTopic, 0);
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
  public void testRunInKafkaHybridModeWithEvents() throws Exception {
    // this config has "kafka.events: true"
    Config config = ConfigFactory.load("HybridKafkaTest/childrenConfigWithEvents.conf");

    String sourceTopic = config.getString("kafka.sourceTopic");
    kafka.createTopic(TopicConfig.withName(sourceTopic).withNumberOfPartitions(1));

    KafkaUtils.createEventTopic(config, "pipeline1", RUN_ID);

    sendDoc("doc1", RUN_ID, sourceTopic);

    WorkerIndexer workerIndexer = new WorkerIndexer();
    LinkedBlockingQueue<Document> pipelineDest = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets = new LinkedBlockingQueue<>();
    Set<String> idSet = CounterUtils.getThreadSafeSet();

    workerIndexer.start(config, "pipeline1", pipelineDest, offsets, true, idSet);

    sendDoc("doc2", RUN_ID, sourceTopic);
    sendDoc("doc3", RUN_ID, sourceTopic);

    CounterUtils.waitUnique(idSet, 9);

    workerIndexer.stop();

    String eventTopicName = KafkaUtils.getEventTopicName("pipeline1", RUN_ID);

    List<KeyValue<String, String>> records = kafka.read(ReadKeyValues
        .from(eventTopicName));

    // there should be 15 events in the event topic: 6 child creation events and 9 indexing events
    assertEquals(15, records.size());

    // the last event should be the indexing event for doc3 (which should be indexed after its children);
    // in hybrid mode, indexing events should have kafka metadata from the source topic as there
    // is no destination topic;
    // within the source topic, doc3 would have offset 2 (as offsets are 0-based) and this
    // same offset should be recorded on the indexing event for doc3
    Event event15 = Event.fromJsonString(records.get(14).getValue());
    assertEquals(sourceTopic, event15.getTopic());
    assertEquals(Integer.valueOf(0), event15.getPartition());
    assertEquals(Long.valueOf(2), event15.getOffset());
    assertEquals(Event.Type.FINISH, event15.getType());
    assertEquals("doc3", event15.getDocumentId());
  }

  @Test
  public void testTwoWorkerIndexerPairs() throws Exception {
    Config config = ConfigFactory.load("HybridKafkaTest/noopConfig.conf");

    String sourceTopic = config.getString("kafka.sourceTopic");
    kafka.createTopic(TopicConfig.withName(sourceTopic).withNumberOfPartitions(2));

    for (int i = 0; i < 500; i++) {
      sendDoc("doc" + i, sourceTopic);
    }

    Set<String> idSet = CounterUtils.getThreadSafeSet();

    RecordingLinkedBlockingQueue<Document> pipelineDest1 =
        new RecordingLinkedBlockingQueue<>();
    RecordingLinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets1 =
        new RecordingLinkedBlockingQueue<>();
    WorkerIndexer workerIndexer1 = new WorkerIndexer();
    workerIndexer1.start(config, "pipeline1", pipelineDest1, offsets1, true, idSet);

    CounterUtils.waitUnique(idSet, 100);

    // start the second worker after the first worker has processed several documents
    // the goal of this is to try to trigger a rebalance scenario where worker 1 is assigned both
    // partitions and then gives one partition up to worker 2 when worker 2 joins the consumer group;
    // we are not testing specific assertions about the rebalance here, we're just trying to trigger
    // it to make sure it doesn't cause anything else to go obviously wrong
    RecordingLinkedBlockingQueue<Document> pipelineDest2 =
        new RecordingLinkedBlockingQueue<>();
    RecordingLinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets2 =
        new RecordingLinkedBlockingQueue<>();
    WorkerIndexer workerIndexer2 = new WorkerIndexer();
    workerIndexer2.start(config, "pipeline1", pipelineDest2, offsets2, true, idSet);

    for (int i = 500; i < 1000; i++) {
      sendDoc("doc" + i, sourceTopic);
    }

    CounterUtils.waitUnique(idSet, 1000);

    workerIndexer1.stop();
    workerIndexer2.stop();

    KafkaConsumer<String, KafkaDocument> consumer = KafkaUtils.createDocumentConsumer(config, "test-client");
    TopicPartition p0 = new TopicPartition(sourceTopic, 0);
    TopicPartition p1 = new TopicPartition(sourceTopic, 1);
    ArrayList partitions = new ArrayList<>();
    partitions.add(p0);
    partitions.add(p1);
    consumer.assign(partitions);

    // the sum of offsets across the two partitions should be the same as the number of documents
    // consumed. All 1000 documents we added to the source topic should have been consumed.
    // however, it is possible that a partition, say partition 1,
    // was reassigned from worker 1 to worker 2 near
    // the end of the run. In this case, worker 2 might have finished reading from that partition 1 and
    // committed the end offset, while worker 1 might have been delayed in committing
    // the offset for its own last read. This will cause the lower offset committed late by worker 1
    // to overwrite the higher offset committed earlier in time by worker 2. Therefore,
    // we can only check >= and not == below
    assertTrue(1000 >= consumer.position(p0) + consumer.position(p1));

    consumer.close();

    // now we compute the maximum offsets added to the offset queue for each worker for each partition;
    // again, these max offsets might be higher than the latest committed offsets
    long worker1MaxP0Offset = 0;
    long worker1MaxP1Offset = 0;
    long worker2MaxP0Offset = 0;
    long worker2MaxP1Offset = 0;

    ArrayList<Map<TopicPartition, OffsetAndMetadata>> offsets1History = offsets1.getHistory();
    ArrayList<Map<TopicPartition, OffsetAndMetadata>> offsets2History = offsets2.getHistory();

    for (Map<TopicPartition, OffsetAndMetadata> map : offsets1History) {
      for (TopicPartition tp : map.keySet()) {
        long offset = map.get(tp).offset();
        if (tp.partition() == 0) {
          if (worker1MaxP0Offset > offset) {
            fail("worker 1 offsets for partition 0 not monotonically increasing");
          } else {
            worker1MaxP0Offset = offset;
          }
        } else if (tp.partition() == 1) {
          if (worker1MaxP1Offset > offset) {
            fail("worker 1 offsets for partition 1 not monotonically increasing");
          } else {
            worker1MaxP1Offset = offset;
          }
        }
      }
    }

    for (Map<TopicPartition, OffsetAndMetadata> map : offsets2History) {
      for (TopicPartition tp : map.keySet()) {
        long offset = map.get(tp).offset();
        if (tp.partition() == 0) {
          if (worker2MaxP0Offset > offset) {
            fail("worker 2 offsets for partition 0 not monotonically increasing");
          } else {
            worker2MaxP0Offset = offset;
          }
        } else if (tp.partition() == 1) {
          if (worker2MaxP1Offset > offset) {
            fail("worker 2 offsets for partition 1 not monotonically increasing");
          } else {
            worker2MaxP1Offset = offset;
          }
        }
      }
    }

    long maxP0Offset = Math.max(worker1MaxP0Offset, worker2MaxP0Offset);
    long maxP1Offset = Math.max(worker1MaxP1Offset, worker2MaxP1Offset);

    // though we weren't able to assert that the sum of the current committed offsets
    // was strictly equal to 1000, we CAN assert that the sum of the
    // max offsets added to the offset queues should equal 1000
    assertEquals(1000, maxP0Offset + maxP1Offset);

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
    for (int i = 0; i < 1000; i++) {
      assertTrue(idsProcessed.contains("doc" + i));
    }

  }

  @Test
  public void testCustomDeserializer() throws Exception {
    // documentDeserializer: "com.kmwllc.com.kmwllc.lucille.core.NonstandardDocumentDeserializer"
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
        new WorkerIndexerPool(config, "pipeline1", true, idSet);

    assertEquals(5, pool.getNumWorkers());

    for (int i = 0; i < 500; i++) {
      sendDoc("doc" + i, sourceTopic);
    }

    pool.start();

    for (int i = 500; i < 1000; i++) {
      sendDoc("doc" + i, sourceTopic);
    }

    CounterUtils.waitUnique(idSet, 1000);

    pool.stop();

    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrapServers"));
    Admin kafkaAdminClient = Admin.create(props);
    Map<TopicPartition, OffsetAndMetadata> retrievedOffsets =
        kafkaAdminClient.listConsumerGroupOffsets(config.getString("kafka.consumerGroupId"))
            .partitionsToOffsetAndMetadata().get();
    TopicPartition partition0 = new TopicPartition(sourceTopic, 0);
    TopicPartition partition1 = new TopicPartition(sourceTopic, 1);
    TopicPartition partition2 = new TopicPartition(sourceTopic, 2);
    TopicPartition partition3 = new TopicPartition(sourceTopic, 3);
    TopicPartition partition4 = new TopicPartition(sourceTopic, 4);

    long sumOffsets = retrievedOffsets.get(partition0).offset() +
        retrievedOffsets.get(partition1).offset() +
        retrievedOffsets.get(partition2).offset() +
        retrievedOffsets.get(partition3).offset() +
        retrievedOffsets.get(partition4).offset();

    // the sum of offsets across the two partitions should be the same as the number of documents
    // consumed. All 1000 documents we added to the source topic should have been consumed.
    assertEquals(1000, sumOffsets);

  }

  @Test
  public void testSourceTopicWildcard() throws Exception {
    // sourceTopic: "test_topic.*"
    Config config = ConfigFactory.load("HybridKafkaTest/sourceTopicWildcard.conf");

    kafka.createTopic(TopicConfig.withName("test_topic1").withNumberOfPartitions(1));
    kafka.createTopic(TopicConfig.withName("test_topic2").withNumberOfPartitions(1));

    sendDoc("doc1", "test_topic1");
    sendDoc("doc2", "test_topic2");

    WorkerIndexer workerIndexer = new WorkerIndexer();

    RecordingLinkedBlockingQueue<Document> pipelineDest =
        new RecordingLinkedBlockingQueue<>();

    RecordingLinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets =
        new RecordingLinkedBlockingQueue<>();

    Set<String> idSet = CounterUtils.getThreadSafeSet();
    workerIndexer.start(config, "pipeline1", pipelineDest, offsets, true, idSet);

    CounterUtils.waitUnique(idSet, 2, 20000, CounterUtils.DEFAULT_END_LAG_MS);

    kafka.createTopic(TopicConfig.withName("test_topic3").withNumberOfPartitions(1));
    kafka.createTopic(TopicConfig.withName("test_topic4").withNumberOfPartitions(1));
    sendDoc("doc3", "test_topic3");
    sendDoc("doc4", "test_topic4");

    // send a second doc to test_topic3
    sendDoc("doc5", "test_topic3");

    CounterUtils.waitUnique(idSet, 5, 3 * 60 * 1000, CounterUtils.DEFAULT_END_LAG_MS);

    workerIndexer.stop();

    // confirm that the consumer group is looking at the correct offset in each topic
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrapServers"));
    Admin kafkaAdminClient = Admin.create(props);
    Map<TopicPartition, OffsetAndMetadata> retrievedOffsets =
        kafkaAdminClient.listConsumerGroupOffsets(config.getString("kafka.consumerGroupId"))
            .partitionsToOffsetAndMetadata().get();
    TopicPartition sourceTopicPartition1 = new TopicPartition("test_topic1", 0);
    assertNotNull(retrievedOffsets.get(sourceTopicPartition1));
    assertEquals(1, retrievedOffsets.get(sourceTopicPartition1).offset());
    TopicPartition sourceTopicPartition2 = new TopicPartition("test_topic2", 0);
    assertNotNull(retrievedOffsets.get(sourceTopicPartition2));
    assertEquals(1, retrievedOffsets.get(sourceTopicPartition2).offset());
    TopicPartition sourceTopicPartition3 = new TopicPartition("test_topic3", 0);
    assertNotNull(retrievedOffsets.get(sourceTopicPartition3));
    assertEquals(2, retrievedOffsets.get(sourceTopicPartition3).offset());
    TopicPartition sourceTopicPartition4 = new TopicPartition("test_topic4", 0);
    assertNotNull(retrievedOffsets.get(sourceTopicPartition4));
    assertEquals(1, retrievedOffsets.get(sourceTopicPartition4).offset());

    // pipelineDest and offset queues should have been drained
    assertEquals(0, pipelineDest.size());
    assertEquals(0, offsets.size());

    // five docs should have been added to pipelineDest queue
    assertEquals(5, pipelineDest.getHistory().size());

    // we should have committed one offset map for the first two topics
    // and a second offset map for the reamining two topics which were created later
    assertEquals(2, offsets.getHistory().size());
    assertEquals(2, offsets.getHistory().get(0).entrySet().size());
    assertEquals(2, offsets.getHistory().get(1).entrySet().size());

    assertEquals(1, offsets.getHistory().get(0).get(sourceTopicPartition1).offset());
    assertEquals(1, offsets.getHistory().get(0).get(sourceTopicPartition2).offset());
    assertEquals(2, offsets.getHistory().get(1).get(sourceTopicPartition3).offset());
    assertEquals(1, offsets.getHistory().get(1).get(sourceTopicPartition4).offset());

  }

  private Document sendDoc(String id, String topic) throws Exception {
    return sendDoc(id, null, topic);
  }

  private Document sendDoc(String id, String runId, String topic) throws Exception {
    List<KeyValue<String, String>> records = new ArrayList<>();
    Document doc = (runId == null) ? Document.create(id) : Document.create(id, runId);
    records.add(new KeyValue<>(id, doc.toString()));
    kafka.send(SendKeyValues.to(topic, records));
    return doc;
  }
}
