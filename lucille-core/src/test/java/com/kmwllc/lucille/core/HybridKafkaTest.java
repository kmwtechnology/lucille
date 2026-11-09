package com.kmwllc.lucille.core;

import com.kmwllc.lucille.util.EmbeddedKafkaUtils;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.kmwllc.lucille.message.KafkaUtils;
import com.kmwllc.lucille.util.CounterUtils;
import com.kmwllc.lucille.util.RecordingLinkedBlockingQueue;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;

import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

public class HybridKafkaTest {

  private static final String RUN_ID = "run1";

  // An embedded instance of Kafka is created for this test class -- not for each test. This allowed the test to run
  // ~10 seconds faster. As such, you need to make sure each test has its OWN, UNIQUE topic name, to prevent interference.
  // A fresh producer / consumer is exposed for each test, however.
  public static EmbeddedKafkaBroker embeddedKafka;

  @BeforeClass
  public static void startKafka() throws Exception {
    // a warmed-up broker, so the one-time creation of __consumer_offsets does not land inside a test's time budget
    embeddedKafka = EmbeddedKafkaUtils.startBroker();
  }


  @AfterClass
  public static void stopKafka() {
    if (embeddedKafka != null) {
      embeddedKafka.destroy();
    }
  }

  private Producer<String, String> producer;
  private Consumer<String, String> consumer;

  @Before
  public void setUp() {
    Map<String, Object> producerProps =
        KafkaTestUtils.producerProps(embeddedKafka);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);

    ProducerFactory<String, String> pf =
        new DefaultKafkaProducerFactory<>(producerProps);
    producer = pf.createProducer();

    Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(embeddedKafka, "alternate_group", false);
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
    consumer = cf.createConsumer();
  }

  private static Config loadTestConfig(String path) {
    String brokers = embeddedKafka.getBrokersAsString();
    Config base = ConfigFactory.load(path);
    return ConfigFactory.empty()
        .withValue("kafka.bootstrapServers", ConfigValueFactory.fromAnyRef(brokers))
        .withFallback(base);
  }

  @Test
  public void testRunInHybridMode() throws Exception {
    Config config = loadTestConfig("HybridKafkaTest/childrenConfig.conf");

    String topicName = config.getString("kafka.sourceTopic");

    embeddedKafka.addTopics(new NewTopic(topicName, 1, (short) 1));

    // send doc - doc1
    sendDoc("doc1", topicName);

    WorkerIndexer workerIndexer = new WorkerIndexer();

    RecordingLinkedBlockingQueue<Document> pipelineDest =
        new RecordingLinkedBlockingQueue<>();

    Set<String> idSet = CounterUtils.getThreadSafeSet();
    workerIndexer.start(config, "pipeline1", pipelineDest, true, idSet);

    // send docs 2 and 3
    sendDoc("doc2", topicName);
    sendDoc("doc3", topicName);

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
    TopicPartition topicPartition = new TopicPartition(topicName, 0);
    assertNotNull(retrievedOffsets.get(topicPartition));
    assertEquals(3, retrievedOffsets.get(topicPartition).offset());

    // the pipelineDest queue should have been drained
    assertEquals(0, pipelineDest.size());

    // three docs should have been added to pipelineDest queue
    // each of the three docs we added should have generated two children
    assertEquals(9, pipelineDest.getHistory().size());

  }

  /**
   * A document the pipeline drops never reaches the Indexer, so no completed batch reports its offset. It must still not
   * hold back the commit: once the documents after it are indexed, the committed offset should move past all three.
   */
  @Test
  public void testDroppedDocumentDoesNotHoldBackTheCommit() throws Exception {
    Config config = loadTestConfig("HybridKafkaTest/dropConfig.conf");
    String topicName = config.getString("kafka.sourceTopic");
    embeddedKafka.addTopics(new NewTopic(topicName, 1, (short) 1));

    Document dropped = Document.create("doc1");
    dropped.setField("drop", "true");
    producer.send(new ProducerRecord<>(topicName, dropped.getId(), dropped.toString()));
    sendDoc("doc2", topicName);
    sendDoc("doc3", topicName);

    WorkerIndexer workerIndexer = new WorkerIndexer();
    Set<String> idSet = CounterUtils.getThreadSafeSet();
    workerIndexer.start(config, "pipeline1", new LinkedBlockingQueue<>(), true, idSet);
    try {
      // doc2 and doc3 are indexed; doc1 is dropped
      CounterUtils.waitUnique(idSet, 2);
      assertEquals("the dropped doc1 at offset 0 must not hold back the commit (-1: nothing committed)", 3,
          awaitCommittedOffset(config, topicName, 3));
    } finally {
      workerIndexer.stop();
    }
  }

  /**
   * A parent the pipeline drops after creating children never reaches the Indexer itself, but its children do. Its offset
   * must be committed once they are indexed.
   */
  @Test
  public void testDroppedParentIsCommittedOnceItsChildrenAreIndexed() throws Exception {
    Config config = loadTestConfig("HybridKafkaTest/dropParentConfig.conf");
    String topicName = config.getString("kafka.sourceTopic");
    embeddedKafka.addTopics(new NewTopic(topicName, 1, (short) 1));
    sendDoc("doc1", topicName);
    sendDoc("doc2", topicName);

    WorkerIndexer workerIndexer = new WorkerIndexer();
    Set<String> idSet = CounterUtils.getThreadSafeSet();
    workerIndexer.start(config, "pipeline1", new LinkedBlockingQueue<>(), true, idSet);
    try {
      // two children of each parent are indexed; the parents are dropped
      CounterUtils.waitUnique(idSet, 4);
      assertEquals("the dropped parents must not hold back the commit (-1: nothing committed)", 2,
          awaitCommittedOffset(config, topicName, 2));
    } finally {
      workerIndexer.stop();
    }
  }

  // The worker commits after each poll, so a commit follows shortly after the documents are indexed. Waits up to 10
  // seconds for the group's committed offset on partition 0 to reach the expected one, and returns the last seen (-1 if
  // nothing was committed).
  private static long awaitCommittedOffset(Config config, String topicName, long expected) throws Exception {
    TopicPartition topicPartition = new TopicPartition(topicName, 0);
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrapServers"));
    try (Admin kafkaAdminClient = Admin.create(props)) {
      long committed = -1;
      long deadline = System.currentTimeMillis() + 10000;
      while (committed != expected && System.currentTimeMillis() < deadline) {
        OffsetAndMetadata offset = kafkaAdminClient.listConsumerGroupOffsets(config.getString("kafka.consumerGroupId"))
            .partitionsToOffsetAndMetadata().get().get(topicPartition);
        committed = offset == null ? -1 : offset.offset();
        if (committed != expected) {
          Thread.sleep(100);
        }
      }
      return committed;
    }
  }

  @Test
  public void testRunInKafkaHybridModeWithEvents() throws Exception {
    // this config has "kafka.events: true"
    Config config = loadTestConfig("HybridKafkaTest/childrenConfigWithEvents.conf");

    String topicName = config.getString("kafka.sourceTopic");

    embeddedKafka.addTopics(new NewTopic(topicName, 1, (short) 1));

    sendDoc("doc1", RUN_ID, topicName);

    WorkerIndexer workerIndexer = new WorkerIndexer();
    LinkedBlockingQueue<Document> pipelineDest = new LinkedBlockingQueue<>();
    Set<String> idSet = CounterUtils.getThreadSafeSet();

    workerIndexer.start(config, "pipeline1", pipelineDest, true, idSet);

    sendDoc("doc2", RUN_ID, topicName);
    sendDoc("doc3", RUN_ID, topicName);

    CounterUtils.waitUnique(idSet, 9);

    workerIndexer.stop();

    String eventTopicName = KafkaUtils.getEventTopicName(config, "pipeline1", RUN_ID);
    consumer.subscribe(List.of(eventTopicName));
    ConsumerRecords<String, String> eventRecords = KafkaTestUtils.getRecords(consumer);

    assertEquals(15, eventRecords.count());
    ConsumerRecord<String, String> record15 = eventRecords.records(new TopicPartition(eventTopicName, 0)).get(14);

    // the last event should be the indexing event for doc3 (which should be indexed after its children);
    // in hybrid mode, indexing events should have kafka metadata from the source topic as there
    // is no destination topic;
    // within the source topic, doc3 would have offset 2 (as offsets are 0-based) and this
    // same offset should be recorded on the indexing event for doc3
    Event event15 = Event.fromJsonString(record15.value());
    assertEquals(topicName, event15.getTopic());
    assertEquals(Integer.valueOf(0), event15.getPartition());
    assertEquals(Long.valueOf(2), event15.getOffset());
    assertEquals(Event.Type.FINISH, event15.getType());
    assertEquals("doc3", event15.getDocumentId());
  }

  @Test
  public void testTwoWorkerIndexerPairs() throws Exception {
    Config config = loadTestConfig("HybridKafkaTest/noopConfig.conf");

    String topicName = config.getString("kafka.sourceTopic");

    embeddedKafka.addTopics(new NewTopic(topicName, 2, (short) 1));

    for (int i = 0; i < 500; i++) {
      sendDoc("doc" + i, topicName);
    }

    Set<String> idSet = CounterUtils.getThreadSafeSet();

    RecordingLinkedBlockingQueue<Document> pipelineDest1 =
        new RecordingLinkedBlockingQueue<>();
    WorkerIndexer workerIndexer1 = new WorkerIndexer();
    workerIndexer1.start(config, "pipeline1", pipelineDest1, true, idSet);

    CounterUtils.waitUnique(idSet, 100);

    // start the second worker after the first worker has processed several documents
    // the goal of this is to try to trigger a rebalance scenario where worker 1 is assigned both
    // partitions and then gives one partition up to worker 2 when worker 2 joins the consumer group;
    // we are not testing specific assertions about the rebalance here, we're just trying to trigger
    // it to make sure it doesn't cause anything else to go obviously wrong
    RecordingLinkedBlockingQueue<Document> pipelineDest2 =
        new RecordingLinkedBlockingQueue<>();
    WorkerIndexer workerIndexer2 = new WorkerIndexer();
    workerIndexer2.start(config, "pipeline1", pipelineDest2, true, idSet);

    for (int i = 500; i < 1000; i++) {
      sendDoc("doc" + i, topicName);
    }

    CounterUtils.waitUnique(idSet, 1000);

    workerIndexer1.stop();
    workerIndexer2.stop();

    KafkaConsumer<String, KafkaDocument> consumer = KafkaUtils.createDocumentConsumer(config, "test-client");
    TopicPartition p0 = new TopicPartition(topicName, 0);
    TopicPartition p1 = new TopicPartition(topicName, 1);
    List<TopicPartition> partitions = new ArrayList<>();
    partitions.add(p0);
    partitions.add(p1);
    consumer.assign(partitions);

    // the committed offsets across the two partitions should add up to the 1000 documents consumed: a worker never
    // commits for a partition it no longer owns, so a late commit by worker 1 cannot rewind worker 2's
    assertEquals(1000, consumer.position(p0) + consumer.position(p1));

    consumer.close();

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
    Set<String> idsProcessed = new HashSet<>();
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
    Config config = loadTestConfig("HybridKafkaTest/customDeserializer.conf");

    String topicName = config.getString("kafka.sourceTopic");

    embeddedKafka.addTopics(new NewTopic(topicName, 1, (short) 1));

    // inputJson does not have the "id" field required for creating a KafkaDocument
    String inputJson = "{\"myId\":\"doc1\", \"field1\":\"value1\"}";

    // we expect the custom deserializer to copy myId to id
    String outputJson = "{\"myId\":\"doc1\",\"field1\":\"value1\",\"id\":\"doc1\"}";

    producer.send(new ProducerRecord<>(topicName, "doc1", inputJson));

    WorkerIndexer workerIndexer = new WorkerIndexer();

    RecordingLinkedBlockingQueue<Document> pipelineDest =
        new RecordingLinkedBlockingQueue<>();

    Set<String> idSet = CounterUtils.getThreadSafeSet();
    workerIndexer.start(config, "pipeline1", pipelineDest, true, idSet);

    CounterUtils.waitUnique(idSet, 1);

    workerIndexer.stop();

    assertEquals(1, pipelineDest.getHistory().size());
    assertEquals(outputJson, pipelineDest.getHistory().get(0).toString());
  }

  @Test
  public void testWorkerIndexerPool() throws Exception {
    Config config = loadTestConfig("HybridKafkaTest/poolConfig.conf");

    String topicName = config.getString("kafka.sourceTopic");

    embeddedKafka.addTopics(new NewTopic(topicName, 5, (short) 1));

    Set<String> idSet = CounterUtils.getThreadSafeSet();
    WorkerIndexerPool pool =
        new WorkerIndexerPool(config, "pipeline1", true, idSet);

    assertEquals(5, pool.getNumWorkers());

    for (int i = 0; i < 500; i++) {
      sendDoc("doc" + i, topicName);
    }

    pool.start();

    for (int i = 500; i < 1000; i++) {
      sendDoc("doc" + i, topicName);
    }

    CounterUtils.waitUnique(idSet, 1000);

    pool.stop();

    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrapServers"));
    Admin kafkaAdminClient = Admin.create(props);
    Map<TopicPartition, OffsetAndMetadata> retrievedOffsets =
        kafkaAdminClient.listConsumerGroupOffsets(config.getString("kafka.consumerGroupId"))
            .partitionsToOffsetAndMetadata().get();
    TopicPartition partition0 = new TopicPartition(topicName, 0);
    TopicPartition partition1 = new TopicPartition(topicName, 1);
    TopicPartition partition2 = new TopicPartition(topicName, 2);
    TopicPartition partition3 = new TopicPartition(topicName, 3);
    TopicPartition partition4 = new TopicPartition(topicName, 4);

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
    // topicName: "test_wildcard_topic.*"
    Config config = loadTestConfig("HybridKafkaTest/sourceTopicWildcard.conf");

    embeddedKafka.addTopics(new NewTopic("test_wildcard_topic1", 1, (short) 1));
    embeddedKafka.addTopics(new NewTopic("test_wildcard_topic2", 1, (short) 1));

    sendDoc("doc1", "test_wildcard_topic1");
    sendDoc("doc2", "test_wildcard_topic2");

    WorkerIndexer workerIndexer = new WorkerIndexer();

    RecordingLinkedBlockingQueue<Document> pipelineDest =
        new RecordingLinkedBlockingQueue<>();

    Set<String> idSet = CounterUtils.getThreadSafeSet();
    workerIndexer.start(config, "pipeline1", pipelineDest, true, idSet);

    CounterUtils.waitUnique(idSet, 2, 20000, CounterUtils.DEFAULT_END_LAG_MS);

    embeddedKafka.addTopics(new NewTopic("test_wildcard_topic3", 1, (short) 1));
    embeddedKafka.addTopics(new NewTopic("test_wildcard_topic4", 1, (short) 1));
    sendDoc("doc3", "test_wildcard_topic3");
    sendDoc("doc4", "test_wildcard_topic4");

    // send a second doc to test_wildcard_topic3
    sendDoc("doc5", "test_wildcard_topic3");

    CounterUtils.waitUnique(idSet, 5, 3 * 60 * 1000, CounterUtils.DEFAULT_END_LAG_MS);

    workerIndexer.stop();

    // confirm that the consumer group is looking at the correct offset in each topic
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrapServers"));
    Admin kafkaAdminClient = Admin.create(props);
    Map<TopicPartition, OffsetAndMetadata> retrievedOffsets =
        kafkaAdminClient.listConsumerGroupOffsets(config.getString("kafka.consumerGroupId"))
            .partitionsToOffsetAndMetadata().get();
    TopicPartition topicPartition1 = new TopicPartition("test_wildcard_topic1", 0);
    assertNotNull(retrievedOffsets.get(topicPartition1));
    assertEquals(1, retrievedOffsets.get(topicPartition1).offset());
    TopicPartition topicPartition2 = new TopicPartition("test_wildcard_topic2", 0);
    assertNotNull(retrievedOffsets.get(topicPartition2));
    assertEquals(1, retrievedOffsets.get(topicPartition2).offset());
    TopicPartition topicPartition3 = new TopicPartition("test_wildcard_topic3", 0);
    assertNotNull(retrievedOffsets.get(topicPartition3));
    assertEquals(2, retrievedOffsets.get(topicPartition3).offset());
    TopicPartition topicPartition4 = new TopicPartition("test_wildcard_topic4", 0);
    assertNotNull(retrievedOffsets.get(topicPartition4));
    assertEquals(1, retrievedOffsets.get(topicPartition4).offset());

    // the pipelineDest queue should have been drained
    assertEquals(0, pipelineDest.size());

    // five docs should have been added to pipelineDest queue
    assertEquals(5, pipelineDest.getHistory().size());

  }

  @Test
  public void testSettingEventTopic() {
    Config config = loadTestConfig("HybridKafkaTest/eventTopic.conf");

    String eventTopic = KafkaUtils.getEventTopicName(config, "pipeline1", RUN_ID);

    assertEquals("test_setting_event_topic", eventTopic);
  }

  private Document sendDoc(String id, String topic) {
    return sendDoc(id, null, topic);
  }

  private Document sendDoc(String id, String runId, String topic) {
    Document doc = (runId == null) ? Document.create(id) : Document.create(id, runId);
    ProducerRecord<String, String> record = new ProducerRecord<>(topic, id, doc.toString());
    producer.send(record);
    return doc;
  }
}
