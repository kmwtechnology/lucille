package com.kmwllc.lucille.connector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

public class KafkaConnectorTest {

  private static final Map<String, Object> BASE_CONFIG_MAP = Map.of(
      "name", "kafkaConnector",
      "class", "com.kmwllc.lucille.connector.KafkaConnector",
      "kafka.bootstrapServers", "localhost:9092",
      "kafka.topic", "test-topic",
      "kafka.consumerGroupId", "test-group",
      "kafka.maxPollIntervalSecs", 600,
      "kafka.clientId", "test-client",
      "continueOnTimeout", false);


  @Test
  public void testOffsetsConfig() throws Exception {
    Map<String, Object> configMap = new HashMap<>(BASE_CONFIG_MAP);
    Map<String, Long> offsets = new HashMap<>();
    offsets.put("0", 100L);
    offsets.put("1", 200L);
    configMap.put("offsets", offsets);
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConsumer<String, Document> mockConsumer = mock(KafkaConsumer.class);
    TopicPartition tp0 = new TopicPartition("test-topic", 0);
    TopicPartition tp1 = new TopicPartition("test-topic", 1);
    when(mockConsumer.assignment()).thenReturn(new HashSet<>(Arrays.asList(tp0, tp1)));
    when(mockConsumer.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());

    KafkaConnector connector = spy(new KafkaConnector(config));
    doReturn(mockConsumer).when(connector).createConsumer(any());

    connector.execute(mock(Publisher.class));

    verify(mockConsumer).seek(tp0, 100L);
    verify(mockConsumer).seek(tp1, 200L);
  }

  @Test
  public void testOffsetsConfigNotSet() throws Exception {
    Map<String, Object> configMap = new HashMap<>(BASE_CONFIG_MAP);
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConsumer<String, Document> mockConsumer = mock(KafkaConsumer.class);
    when(mockConsumer.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());

    KafkaConnector connector = spy(new KafkaConnector(config));
    doReturn(mockConsumer).when(connector).createConsumer(any());

    connector.execute(mock(Publisher.class));

    verify(mockConsumer, times(0)).seek(any(), any(Long.class));
  }

  @Test
  public void testMaxMessagesConfig() throws Exception {
    Map<String, Object> configMap = new HashMap<>(BASE_CONFIG_MAP);
    configMap.put("maxMessages", 1L);
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConsumer<String, Document> mockConsumer = mock(KafkaConsumer.class);
    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");
    ConsumerRecord<String, Document> record1 = new ConsumerRecord<>("test-topic", 0, 0, "key1", doc1);
    ConsumerRecord<String, Document> record2 = new ConsumerRecord<>("test-topic", 0, 1, "key2", doc2);

    TopicPartition tp = new TopicPartition("test-topic", 0);
    Map<TopicPartition, List<ConsumerRecord<String, Document>>> recordsMap = new HashMap<>();
    recordsMap.put(tp, Arrays.asList(record1, record2));
    ConsumerRecords<String, Document> records = new ConsumerRecords<>(recordsMap);

    when(mockConsumer.poll(any(Duration.class))).thenReturn(records);

    KafkaConnector connector = spy(new KafkaConnector(config));
    doReturn(mockConsumer).when(connector).createConsumer(any());

    Publisher mockPublisher = mock(Publisher.class);
    connector.execute(mockPublisher);

    verify(mockPublisher, times(1)).publish(any(Document.class));
  }

  @Test
  public void testMaxMessagesConfigNotSet() throws Exception {
    Map<String, Object> configMap = new HashMap<>(BASE_CONFIG_MAP);
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConsumer<String, Document> mockConsumer = mock(KafkaConsumer.class);
    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");
    ConsumerRecord<String, Document> record1 = new ConsumerRecord<>("test-topic", 0, 0, "key1", doc1);
    ConsumerRecord<String, Document> record2 = new ConsumerRecord<>("test-topic", 0, 1, "key2", doc2);

    TopicPartition tp = new TopicPartition("test-topic", 0);
    Map<TopicPartition, List<ConsumerRecord<String, Document>>> recordsMap = new HashMap<>();
    recordsMap.put(tp, Arrays.asList(record1, record2));
    ConsumerRecords<String, Document> records = new ConsumerRecords<>(recordsMap);

    when(mockConsumer.poll(any(Duration.class))).thenReturn(records).thenReturn(ConsumerRecords.empty());

    KafkaConnector connector = spy(new KafkaConnector(config));
    doReturn(mockConsumer).when(connector).createConsumer(any());

    Publisher mockPublisher = mock(Publisher.class);
    connector.execute(mockPublisher);

    verify(mockPublisher, times(2)).publish(any(Document.class));
  }

  @Test
  public void testMessageTimeoutConfig() throws Exception {
    Map<String, Object> configMap = new HashMap<>(BASE_CONFIG_MAP);
    configMap.put("messageTimeout", 500L);
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConsumer<String, Document> mockConsumer = mock(KafkaConsumer.class);
    when(mockConsumer.poll(Duration.ofMillis(500L))).thenReturn(ConsumerRecords.empty());

    KafkaConnector connector = spy(new KafkaConnector(config));
    doReturn(mockConsumer).when(connector).createConsumer(any());

    connector.execute(mock(Publisher.class));

    verify(mockConsumer).poll(Duration.ofMillis(500L));
  }

  @Test
  public void testMessageTimeoutConfigNotSet() throws Exception {
    Map<String, Object> configMap = new HashMap<>(BASE_CONFIG_MAP);
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConsumer<String, Document> mockConsumer = mock(KafkaConsumer.class);
    when(mockConsumer.poll(Duration.ofMillis(100L))).thenReturn(ConsumerRecords.empty());

    KafkaConnector connector = spy(new KafkaConnector(config));
    doReturn(mockConsumer).when(connector).createConsumer(any());

    connector.execute(mock(Publisher.class));

    verify(mockConsumer).poll(Duration.ofMillis(100L));
  }

  @Test
  public void testHandleMessage() throws Exception {
    Map<String, Object> configMap = new HashMap<>(BASE_CONFIG_MAP);
    Config config = ConfigFactory.parseMap(configMap);

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");
    KafkaConnector connector = new KafkaConnector(config);

    Document doc = Document.create("doc1");
    doc.setField("field1", "value1");
    ConsumerRecord<String, Document> record = new ConsumerRecord<>("test-topic", 0, 0, "key", doc);

    connector.handleMessage(record, publisher);

    List<Document> docs = messenger.getDocsSentForProcessing();
    assertEquals(1, docs.size());
    assertEquals("doc1", docs.get(0).getId());
    assertEquals("value1", docs.get(0).getString("field1"));
  }

  @Test
  public void testClose() throws Exception {
    Map<String, Object> configMap = new HashMap<>(BASE_CONFIG_MAP);
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConsumer<String, Document> mockConsumer = mock(KafkaConsumer.class);
    when(mockConsumer.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());

    KafkaConnector connector = spy(new KafkaConnector(config));
    doReturn(mockConsumer).when(connector).createConsumer(any());

    connector.execute(mock(Publisher.class));
    connector.close();

    verify(mockConsumer).wakeup();
    verify(mockConsumer).close();
  }

  @Test
  public void testExecuteMaxMessages() throws Exception {
    Map<String, Object> configMap = new HashMap<>(BASE_CONFIG_MAP);
    configMap.put("maxMessages", 2L);
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConsumer<String, Document> mockConsumer = mock(KafkaConsumer.class);
    KafkaConnector connector = spy(new KafkaConnector(config));
    doReturn(mockConsumer).when(connector).createConsumer(any());

    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");
    ConsumerRecord<String, Document> record1 = new ConsumerRecord<>("test-topic", 0, 0, "key1", doc1);
    ConsumerRecord<String, Document> record2 = new ConsumerRecord<>("test-topic", 0, 1, "key2", doc2);

    TopicPartition tp = new TopicPartition("test-topic", 0);
    Map<TopicPartition, List<ConsumerRecord<String, Document>>> recordsMap = new HashMap<>();
    recordsMap.put(tp, Arrays.asList(record1, record2));
    ConsumerRecords<String, Document> records = new ConsumerRecords<>(recordsMap);

    when(mockConsumer.poll(any(Duration.class))).thenReturn(records);

    Publisher mockPublisher = mock(Publisher.class);
    connector.execute(mockPublisher);

    verify(mockPublisher, times(2)).publish(any(Document.class));

  }

  @Test
  public void testExecuteContinueOnTimeoutFalse() throws Exception {
    Map<String, Object> configMap = new HashMap<>(BASE_CONFIG_MAP);
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConsumer<String, Document> mockConsumer = mock(KafkaConsumer.class);
    KafkaConnector connector = spy(new KafkaConnector(config));
    doReturn(mockConsumer).when(connector).createConsumer(any());

    when(mockConsumer.poll(any(Duration.class))).thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

    Publisher mockPublisher = mock(Publisher.class);
    connector.execute(mockPublisher);

    verify(mockConsumer, times(1)).poll(any(Duration.class));

  }

  @Test
  public void testHandleMessagePublishException() throws Exception {
    Map<String, Object> configMap = new HashMap<>(BASE_CONFIG_MAP);
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConnector connector = new KafkaConnector(config);
    Document doc = Document.create("doc1");
    ConsumerRecord<String, Document> record = new ConsumerRecord<>("test-topic", 0, 0, "key", doc);

    Publisher mockPublisher = mock(Publisher.class);
    doThrow(new RuntimeException("Publish failed")).when(mockPublisher).publish(any(Document.class));

    assertThrows(ConnectorException.class, () -> connector.handleMessage(record, mockPublisher));
  }

  @Test
  public void testExecuteWithOffsets() throws Exception {
    Map<String, Object> configMap = new HashMap<>(BASE_CONFIG_MAP);
    Map<String, Long> offsets = new HashMap<>();
    offsets.put("0", 100L);
    configMap.put("offsets", offsets);
    configMap.put("maxMessages", 0L); // Stop immediately
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConsumer<String, Document> mockConsumer = mock(KafkaConsumer.class);
    TopicPartition tp0 = new TopicPartition("test-topic", 0);
    Set<TopicPartition> assignment = new HashSet<>();
    assignment.add(tp0);
    when(mockConsumer.assignment()).thenReturn(assignment);
    when(mockConsumer.poll(Duration.ZERO)).thenReturn(new ConsumerRecords<>(Collections.emptyMap()));
    when(mockConsumer.poll(any(Duration.class))).thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

    KafkaConnector connector = spy(new KafkaConnector(config));
    doReturn(mockConsumer).when(connector).createConsumer(any());

    connector.execute(mock(Publisher.class));

    verify(mockConsumer).seek(tp0, 100L);
  }

  @Test
  public void testThreadTerminatesUponClose() throws Exception {
    Map<String, Object> configMap = new HashMap<>(BASE_CONFIG_MAP);
    configMap.put("messageTimeout", 10000L); // Long timeout
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConsumer<String, Document> mockConsumer = mock(KafkaConsumer.class);
    CountDownLatch pollLatch = new CountDownLatch(1);

    // Mock poll to wait until the latch is released
    doAnswer(invocation -> {
      pollLatch.await();
      return ConsumerRecords.empty();
    }).when(mockConsumer).poll(any(Duration.class));

    // Mock wakeup to release the latch, simulating poll returning or interrupted
    doAnswer(invocation -> {
      pollLatch.countDown();
      return null;
    }).when(mockConsumer).wakeup();

    KafkaConnector connector = spy(new KafkaConnector(config));
    doReturn(mockConsumer).when(connector).createConsumer(any());

    Thread connectorThread = new Thread(() -> {
      try {
        connector.execute(mock(Publisher.class));
      } catch (ConnectorException e) {
        // expected when connector is interrupted
      }
    });

    connectorThread.start();
    Thread.sleep(200);
    connector.close();
    connectorThread.join(config.getLong("messageTimeout"));

    assertEquals(Thread.State.TERMINATED, connectorThread.getState());
    verify(mockConsumer, times(1)).poll(any(Duration.class));
  }

  @Test
  public void testThreadTerminatesUponPollTimedOut() throws Exception {
    Map<String, Object> configMap = new HashMap<>(BASE_CONFIG_MAP);
    configMap.put("messageTimeout", 10L);
    // continue on timeout is false, after the poll times out the connector should terminate
    configMap.put("continueOnTimeout", false);
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConsumer<String, Document> mockConsumer = mock(KafkaConsumer.class);

    // Mock poll to wait LONGER than messageTimeout
    doAnswer(invocation -> {
      Thread.sleep(config.getLong("messageTimeout") + 10L);
      return ConsumerRecords.empty();
    }).when(mockConsumer).poll(any(Duration.class));

    KafkaConnector connector = spy(new KafkaConnector(config));
    doReturn(mockConsumer).when(connector).createConsumer(any());

    Thread connectorThread = new Thread(() -> {
      try {
        connector.execute(mock(Publisher.class));
      } catch (ConnectorException e) {
      }
    });

    connectorThread.start();
    connectorThread.join();
    assertEquals(Thread.State.TERMINATED, connectorThread.getState());
    verify(mockConsumer, times(1)).poll(any(Duration.class));
  }

  @Test
  public void testExecuteExceptionHandling() throws Exception {
    Map<String, Object> configMap = new HashMap<>(BASE_CONFIG_MAP);
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConsumer<String, Document> mockConsumer = mock(KafkaConsumer.class);
    when(mockConsumer.poll(any(Duration.class))).thenThrow(new RuntimeException("Kafka error"));

    KafkaConnector connector = spy(new KafkaConnector(config));
    doReturn(mockConsumer).when(connector).createConsumer(any());

    assertThrows(ConnectorException.class, () -> connector.execute(mock(Publisher.class)));

  }

  @Test
  public void testDeserializerWithDocIdPrefix() throws Exception {
    KafkaConnectorDefaultDeserializer deserializer = new KafkaConnectorDefaultDeserializer();
    Map<String, Object> kafkaConfigs = new HashMap<>();
    kafkaConfigs.put("docIdPrefix", "prefix_");
    deserializer.configure(kafkaConfigs, false);

    String jsonValue = "{\"id\":\"doc1\"}";
    Document doc = deserializer.deserialize("test-topic", jsonValue.getBytes());
    assertEquals("prefix_doc1", doc.getId());
  }

  @Test
  public void testDeserializerWithIdField() throws Exception {
    KafkaConnectorDefaultDeserializer deserializer = new KafkaConnectorDefaultDeserializer();
    Map<String, Object> kafkaConfigs = new HashMap<>();
    kafkaConfigs.put("idField", "myIdField");
    deserializer.configure(kafkaConfigs, false);

    String jsonValue = "{\"id\":\"doc1\", \"myIdField\":\"doc2\"}";
    Document doc = deserializer.deserialize("test-topic", jsonValue.getBytes());
    assertEquals("doc2", doc.getId());
  }

  @Test
  public void testCustomDeserializer() throws Exception {
    Map<String, Object> configMap = new HashMap<>(BASE_CONFIG_MAP);
    configMap.put("kafka.documentDeserializer", "com.kmwllc.lucille.connector.KafkaConnectorTest$TestCustomDeserializer");
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConnector connector = new KafkaConnector(config);
    Properties props = new Properties();
    connector.enhanceConsumerProperties(props, config);

    assertEquals("com.kmwllc.lucille.connector.KafkaConnectorTest$TestCustomDeserializer",
        props.getProperty("value.deserializer"));

    CustomDeserializer deserializer = new CustomDeserializer();
    Map<String, Object> kafkaConfigs = new HashMap<>();
    kafkaConfigs.put("docIdPrefix", "");
    deserializer.configure(kafkaConfigs, false);
    String jsonValue = "{\"id\":\"doc1\"}";
    Document doc = deserializer.deserialize("test-topic", jsonValue.getBytes());
    assertEquals("doc1", doc.getId());
    assertEquals("123", doc.getString("test"));
  }

  @Test
  public void testEnhanceConsumerPropertiesOverride() throws Exception {
    Map<String, Object> configMap = new HashMap<>(BASE_CONFIG_MAP);
    configMap.put("kafka.documentDeserializer", "com.kmwllc.lucille.connector.KafkaConnectorTest$TestDeserializerWithProperty");
    Config config = ConfigFactory.parseMap(configMap);

    ConnectorWithProperty connector = new ConnectorWithProperty(config);
    Properties props = new Properties();
    connector.enhanceConsumerProperties(props, config);

    assertEquals("com.kmwllc.lucille.connector.KafkaConnectorTest$TestDeserializerWithProperty",
        props.getProperty("value.deserializer"));
    assertEquals("test123", props.getProperty("testProperty"));

    DeserializerWithProperty deserializer = new DeserializerWithProperty();
    Map<String, Object> kafkaConfigs = new HashMap<>();
    kafkaConfigs.put("testProperty", "test123000"); // Use a different value to ensure it's read from configs
    deserializer.configure(kafkaConfigs, false);
    String jsonValue = "{\"id\":\"doc1\"}";
    Document doc = deserializer.deserialize("test-topic", jsonValue.getBytes());
    assertEquals("doc1", doc.getId());
    assertEquals("test123000", doc.getString("test"));
  }

  public static class CustomDeserializer extends KafkaConnectorDefaultDeserializer {

    @Override
    public Document deserialize(String topic, byte[] data) {
      Document doc = super.deserialize(topic, data);
      if (doc != null) {
        doc.setField("test", "123");
      }
      return doc;
    }
  }

  public static class DeserializerWithProperty extends KafkaConnectorDefaultDeserializer {

    private String testPropertyValue;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
      super.configure(configs, isKey);
      this.testPropertyValue = (String) configs.get("testProperty");
    }

    @Override
    public Document deserialize(String topic, byte[] data) {
      Document doc = super.deserialize(topic, data);
      if (doc != null) {
        doc.setField("test", testPropertyValue);
      }
      return doc;
    }
  }

  public static class ConnectorWithProperty extends KafkaConnector {

    public static final Spec SPEC = KafkaConnector.SPEC;

    public ConnectorWithProperty(Config config) {
      super(config);
    }

    @Override
    public void enhanceConsumerProperties(Properties props, Config config) {
      super.enhanceConsumerProperties(props, config);
      props.put("testProperty", "test123");
    }
  }
}
