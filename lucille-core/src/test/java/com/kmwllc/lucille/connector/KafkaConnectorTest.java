package com.kmwllc.lucille.connector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

public class KafkaConnectorTest {

  @Test
  public void testAsDoc() throws Exception {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("name", "kafkaConnector");
    configMap.put("class", "com.kmwllc.lucille.connector.KafkaConnector");
    configMap.put("kafka.bootstrapServers", "localhost:9092");
    configMap.put("kafka.topic", "test-topic");
    configMap.put("kafka.consumerGroupId", "test-group");
    configMap.put("kafka.maxPollIntervalSecs", 600);
    configMap.put("kafka.clientId", "test-client");

    Config config = ConfigFactory.parseMap(configMap);

    KafkaConnector connector = new KafkaConnector(config);
    String jsonValue = "{\"id\":\"doc1\", \"field1\":\"value1\"}";
    ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0, "key", jsonValue);

    Document doc = connector.asDoc(record);
    assertEquals("doc1", doc.getId());
    assertEquals("value1", doc.getString("field1"));
  }

  @Test
  public void testAsDocWithIdField() throws Exception {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("name", "kafkaConnector");
    configMap.put("class", "com.kmwllc.lucille.connector.KafkaConnector");
    configMap.put("kafka.bootstrapServers", "localhost:9092");
    configMap.put("kafka.topic", "test-topic");
    configMap.put("kafka.consumerGroupId", "test-group");
    configMap.put("kafka.maxPollIntervalSecs", 600);
    configMap.put("kafka.clientId", "test-client");
    configMap.put("idField", "myIdField");
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConnector connector = new KafkaConnector(config);
    String jsonValue = "{\"id\":\"doc1\", \"myIdField\":\"doc2\", \"field1\":\"value1\"}";
    ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0, "key", jsonValue);

    Document doc = connector.asDoc(record);
    assertEquals("doc2", doc.getId());
    assertEquals("value1", doc.getString("field1"));
  }

  @Test
  public void testOffsetsConfig() throws Exception {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("name", "kafkaConnector");
    configMap.put("class", "com.kmwllc.lucille.connector.KafkaConnector");
    configMap.put("kafka.bootstrapServers", "localhost:9092");
    configMap.put("kafka.topic", "test-topic");
    configMap.put("kafka.consumerGroupId", "test-group");
    configMap.put("kafka.maxPollIntervalSecs", 600);
    configMap.put("kafka.clientId", "test-client");
    Map<String, Long> offsets = new HashMap<>();
    offsets.put("0", 100L);
    offsets.put("1", 200L);
    configMap.put("offsets", offsets);
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConnector connector = new KafkaConnector(config);
    java.lang.reflect.Field offsetsField = KafkaConnector.class.getDeclaredField("offsets");
    offsetsField.setAccessible(true);
    Map<Integer, Long> actualOffsets = (Map<Integer, Long>) offsetsField.get(connector);
    assertEquals(100L, (long) actualOffsets.get(0));
    assertEquals(200L, (long) actualOffsets.get(1));
  }

  @Test
  public void testOffsetsConfigNotSet() throws Exception {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("name", "kafkaConnector");
    configMap.put("class", "com.kmwllc.lucille.connector.KafkaConnector");
    configMap.put("kafka.bootstrapServers", "localhost:9092");
    configMap.put("kafka.topic", "test-topic");
    configMap.put("kafka.consumerGroupId", "test-group");
    configMap.put("kafka.maxPollIntervalSecs", 600);
    configMap.put("kafka.clientId", "test-client");
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConnector connector = new KafkaConnector(config);
    java.lang.reflect.Field offsetsField = KafkaConnector.class.getDeclaredField("offsets");
    offsetsField.setAccessible(true);
    org.junit.Assert.assertNull(offsetsField.get(connector));
  }

  @Test
  public void testMaxMessagesConfig() throws Exception {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("name", "kafkaConnector");
    configMap.put("class", "com.kmwllc.lucille.connector.KafkaConnector");
    configMap.put("kafka.bootstrapServers", "localhost:9092");
    configMap.put("kafka.topic", "test-topic");
    configMap.put("kafka.consumerGroupId", "test-group");
    configMap.put("kafka.maxPollIntervalSecs", 600);
    configMap.put("kafka.clientId", "test-client");
    configMap.put("maxMessages", 10L);
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConnector connector = new KafkaConnector(config);
    java.lang.reflect.Field maxMessagesField = KafkaConnector.class.getDeclaredField("maxMessages");
    maxMessagesField.setAccessible(true);
    assertEquals(10L, maxMessagesField.get(connector));
  }

  @Test
  public void testMaxMessagesConfigNotSet() throws Exception {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("name", "kafkaConnector");
    configMap.put("class", "com.kmwllc.lucille.connector.KafkaConnector");
    configMap.put("kafka.bootstrapServers", "localhost:9092");
    configMap.put("kafka.topic", "test-topic");
    configMap.put("kafka.consumerGroupId", "test-group");
    configMap.put("kafka.maxPollIntervalSecs", 600);
    configMap.put("kafka.clientId", "test-client");
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConnector connector = new KafkaConnector(config);
    java.lang.reflect.Field maxMessagesField = KafkaConnector.class.getDeclaredField("maxMessages");
    maxMessagesField.setAccessible(true);
    org.junit.Assert.assertNull(maxMessagesField.get(connector));
  }

  @Test
  public void testMessageTimeoutConfig() throws Exception {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("name", "kafkaConnector");
    configMap.put("class", "com.kmwllc.lucille.connector.KafkaConnector");
    configMap.put("kafka.bootstrapServers", "localhost:9092");
    configMap.put("kafka.topic", "test-topic");
    configMap.put("kafka.consumerGroupId", "test-group");
    configMap.put("kafka.clientId", "test-client");
    configMap.put("kafka.maxPollIntervalSecs", 600);
    configMap.put("messageTimeout", 500L);
    configMap.put("continueOnTimeout", false);
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConnector connector = new KafkaConnector(config);
    java.lang.reflect.Field messageTimeoutField = KafkaConnector.class.getDeclaredField("messageTimeout");
    messageTimeoutField.setAccessible(true);
    assertEquals(500L, messageTimeoutField.get(connector));

    java.lang.reflect.Field continueOnTimeoutField = KafkaConnector.class.getDeclaredField("continueOnTimeout");
    continueOnTimeoutField.setAccessible(true);
    assertEquals(false, continueOnTimeoutField.get(connector));
  }

  @Test
  public void testMessageTimeoutConfigNotSet() throws Exception {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("name", "kafkaConnector");
    configMap.put("class", "com.kmwllc.lucille.connector.KafkaConnector");
    configMap.put("kafka.bootstrapServers", "localhost:9092");
    configMap.put("kafka.topic", "test-topic");
    configMap.put("kafka.consumerGroupId", "test-group");
    configMap.put("kafka.maxPollIntervalSecs", 600);
    configMap.put("kafka.clientId", "test-client");
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConnector connector = new KafkaConnector(config);
    java.lang.reflect.Field messageTimeoutField = KafkaConnector.class.getDeclaredField("messageTimeout");
    messageTimeoutField.setAccessible(true);
    assertEquals(100L, messageTimeoutField.get(connector));

    java.lang.reflect.Field continueOnTimeoutField = KafkaConnector.class.getDeclaredField("continueOnTimeout");
    continueOnTimeoutField.setAccessible(true);
    assertEquals(true, continueOnTimeoutField.get(connector));
  }

  @Test
  public void testHandleMessage() throws Exception {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("name", "kafkaConnector");
    configMap.put("class", "com.kmwllc.lucille.connector.KafkaConnector");
    configMap.put("kafka.bootstrapServers", "localhost:9092");
    configMap.put("kafka.topic", "test-topic");
    configMap.put("kafka.consumerGroupId", "test-group");
    configMap.put("kafka.maxPollIntervalSecs", 600);
    configMap.put("kafka.clientId", "test-client");
    Config config = ConfigFactory.parseMap(configMap);

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");
    KafkaConnector connector = new KafkaConnector(config);

    String jsonValue = "{\"id\":\"doc1\", \"field1\":\"value1\"}";
    ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0, "key", jsonValue);

    connector.handleMessage(record, publisher);

    List<Document> docs = messenger.getDocsSentForProcessing();
    assertEquals(1, docs.size());
    assertEquals("doc1", docs.get(0).getId());
    assertEquals("value1", docs.get(0).getString("field1"));
  }

  @Test
  public void testClose() throws Exception {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("name", "kafkaConnector");
    configMap.put("class", "com.kmwllc.lucille.connector.KafkaConnector");
    configMap.put("kafka.bootstrapServers", "localhost:9092");
    configMap.put("kafka.topic", "test-topic");
    configMap.put("kafka.consumerGroupId", "test-group");
    configMap.put("kafka.maxPollIntervalSecs", 600);
    configMap.put("kafka.clientId", "test-client");
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConnector connector = new KafkaConnector(config);

    // Initial state
    java.lang.reflect.Field runningField = KafkaConnector.class.getDeclaredField("running");
    runningField.setAccessible(true);
    assertEquals(true, runningField.get(connector));

    // Close without initializing consumer
    connector.close();
    assertEquals(false, runningField.get(connector));

    // Check that consumer is null
    java.lang.reflect.Field consumerField = KafkaConnector.class.getDeclaredField("consumer");
    consumerField.setAccessible(true);
    assertNull(consumerField.get(connector));
  }

  @Test
  public void testExecuteMaxMessages() throws Exception {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("name", "kafkaConnector");
    configMap.put("class", "com.kmwllc.lucille.connector.KafkaConnector");
    configMap.put("kafka.bootstrapServers", "localhost:9092");
    configMap.put("kafka.topic", "test-topic");
    configMap.put("kafka.consumerGroupId", "test-group");
    configMap.put("kafka.maxPollIntervalSecs", 600);
    configMap.put("kafka.clientId", "test-client");
    configMap.put("maxMessages", 2L);
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConsumer<String, String> mockConsumer = mock(KafkaConsumer.class);
    KafkaConnector connector = new KafkaConnector(config) {
      @Override
      KafkaConsumer<String, String> createConsumer(Properties props) {
        return mockConsumer;
      }
    };

    String jsonValue1 = "{\"id\":\"doc1\"}";
    String jsonValue2 = "{\"id\":\"doc2\"}";
    ConsumerRecord<String, String> record1 = new ConsumerRecord<>("test-topic", 0, 0, "key1", jsonValue1);
    ConsumerRecord<String, String> record2 = new ConsumerRecord<>("test-topic", 0, 1, "key2", jsonValue2);

    TopicPartition tp = new TopicPartition("test-topic", 0);
    Map<TopicPartition, List<ConsumerRecord<String, String>>> recordsMap = new HashMap<>();
    recordsMap.put(tp, Arrays.asList(record1, record2));
    ConsumerRecords<String, String> records = new ConsumerRecords<>(recordsMap);

    when(mockConsumer.poll(any(Duration.class))).thenReturn(records);

    Publisher mockPublisher = mock(Publisher.class);
    connector.execute(mockPublisher);

    verify(mockPublisher, times(2)).publish(any(Document.class));
    verify(mockConsumer).close();
  }

  @Test
  public void testExecuteContinueOnTimeoutFalse() throws Exception {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("name", "kafkaConnector");
    configMap.put("class", "com.kmwllc.lucille.connector.KafkaConnector");
    configMap.put("kafka.bootstrapServers", "localhost:9092");
    configMap.put("kafka.topic", "test-topic");
    configMap.put("kafka.consumerGroupId", "test-group");
    configMap.put("kafka.maxPollIntervalSecs", 600);
    configMap.put("kafka.clientId", "test-client");
    configMap.put("continueOnTimeout", false);
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConsumer<String, String> mockConsumer = mock(KafkaConsumer.class);
    KafkaConnector connector = new KafkaConnector(config) {
      @Override
      KafkaConsumer<String, String> createConsumer(Properties props) {
        return mockConsumer;
      }
    };

    when(mockConsumer.poll(any(Duration.class))).thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

    Publisher mockPublisher = mock(Publisher.class);
    connector.execute(mockPublisher);

    verify(mockConsumer, times(1)).poll(any(Duration.class));
    verify(mockConsumer).close();
  }

  @Test
  public void testAsDocNonObject() throws Exception {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("name", "kafkaConnector");
    configMap.put("class", "com.kmwllc.lucille.connector.KafkaConnector");
    configMap.put("kafka.bootstrapServers", "localhost:9092");
    configMap.put("kafka.topic", "test-topic");
    configMap.put("kafka.consumerGroupId", "test-group");
    configMap.put("kafka.maxPollIntervalSecs", 600);
    configMap.put("kafka.clientId", "test-client");
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConnector connector = new KafkaConnector(config);
    ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0, "key", "\"not an object\"");

    assertThrows(com.kmwllc.lucille.core.ConnectorException.class, () -> connector.asDoc(record));
  }

  @Test
  public void testHandleMessagePublishException() throws Exception {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("name", "kafkaConnector");
    configMap.put("class", "com.kmwllc.lucille.connector.KafkaConnector");
    configMap.put("kafka.bootstrapServers", "localhost:9092");
    configMap.put("kafka.topic", "test-topic");
    configMap.put("kafka.consumerGroupId", "test-group");
    configMap.put("kafka.maxPollIntervalSecs", 600);
    configMap.put("kafka.clientId", "test-client");
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConnector connector = new KafkaConnector(config);
    String jsonValue = "{\"id\":\"doc1\"}";
    ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0, "key", jsonValue);

    Publisher mockPublisher = mock(Publisher.class);
    doThrow(new RuntimeException("Publish failed")).when(mockPublisher).publish(any(Document.class));

    assertThrows(com.kmwllc.lucille.core.ConnectorException.class, () -> connector.handleMessage(record, mockPublisher));
  }

  @Test
  public void testExecuteWithOffsets() throws Exception {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("name", "kafkaConnector");
    configMap.put("class", "com.kmwllc.lucille.connector.KafkaConnector");
    configMap.put("kafka.bootstrapServers", "localhost:9092");
    configMap.put("kafka.topic", "test-topic");
    configMap.put("kafka.consumerGroupId", "test-group");
    configMap.put("kafka.maxPollIntervalSecs", 600);
    configMap.put("kafka.clientId", "test-client");
    Map<String, Long> offsets = new HashMap<>();
    offsets.put("0", 100L);
    configMap.put("offsets", offsets);
    configMap.put("maxMessages", 0L); // Stop immediately
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConsumer<String, String> mockConsumer = mock(KafkaConsumer.class);
    TopicPartition tp0 = new TopicPartition("test-topic", 0);
    Set<TopicPartition> assignment = new HashSet<>();
    assignment.add(tp0);
    when(mockConsumer.assignment()).thenReturn(assignment);
    when(mockConsumer.poll(Duration.ZERO)).thenReturn(new ConsumerRecords<>(Collections.emptyMap()));
    when(mockConsumer.poll(any(Duration.class))).thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

    KafkaConnector connector = new KafkaConnector(config) {
      @Override
      KafkaConsumer<String, String> createConsumer(Properties props) {
        return mockConsumer;
      }
    };

    connector.execute(mock(Publisher.class));

    verify(mockConsumer).seek(tp0, 100L);
  }

  @Test
  public void testExecuteExceptionHandling() throws Exception {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("name", "kafkaConnector");
    configMap.put("class", "com.kmwllc.lucille.connector.KafkaConnector");
    configMap.put("kafka.bootstrapServers", "localhost:9092");
    configMap.put("kafka.topic", "test-topic");
    configMap.put("kafka.consumerGroupId", "test-group");
    configMap.put("kafka.maxPollIntervalSecs", 600);
    configMap.put("kafka.clientId", "test-client");
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConsumer<String, String> mockConsumer = mock(KafkaConsumer.class);
    when(mockConsumer.poll(any(Duration.class))).thenThrow(new RuntimeException("Kafka error"));

    KafkaConnector connector = new KafkaConnector(config) {
      @Override
      KafkaConsumer<String, String> createConsumer(Properties props) {
        return mockConsumer;
      }
    };

    assertThrows(com.kmwllc.lucille.core.ConnectorException.class, () -> connector.execute(mock(Publisher.class)));
    verify(mockConsumer).close();
  }

  @Test
  public void testAsDocWithDocIdPrefix() throws Exception {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("name", "kafkaConnector");
    configMap.put("class", "com.kmwllc.lucille.connector.KafkaConnector");
    configMap.put("kafka.bootstrapServers", "localhost:9092");
    configMap.put("kafka.topic", "test-topic");
    configMap.put("kafka.consumerGroupId", "test-group");
    configMap.put("kafka.maxPollIntervalSecs", 600);
    configMap.put("kafka.clientId", "test-client");
    configMap.put("docIdPrefix", "prefix_");
    Config config = ConfigFactory.parseMap(configMap);

    KafkaConnector connector = new KafkaConnector(config);
    String jsonValue = "{\"id\":\"doc1\"}";
    ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0, "key", jsonValue);

    Document doc = connector.asDoc(record);
    assertEquals("prefix_doc1", doc.getId());
  }
}
