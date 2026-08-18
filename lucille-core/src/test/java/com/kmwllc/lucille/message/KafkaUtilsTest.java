package com.kmwllc.lucille.message;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Properties;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KafkaUtilsTest {

  @Test
  public void testCreateConsumerProps() {
    Config directConfig = ConfigFactory.load("KafkaUtilsTest/consumer-conf/direct.conf");
    Config externalConfig = ConfigFactory.load("KafkaUtilsTest/consumer-conf/external.conf");
    Properties directProps = KafkaUtils.createConsumerProps(directConfig, "test-client-1");
    Properties externalProps = KafkaUtils.createConsumerProps(externalConfig, "test-client-1");
    assertThat(directProps.size(), equalTo(externalProps.size()));
    for (Object key : directProps.keySet()) {
      assertThat(String.format("%s should be present in both configs.", key), externalProps.containsKey(key), equalTo(true));
      assertThat(String.format("%s should match.", key), directProps.get(key.toString()).toString(),
          equalTo(externalProps.get(key.toString()).toString()));
    }
  }

  @Test
  public void testCreateProducerProps() {
    Config directConfig = ConfigFactory.load("KafkaUtilsTest/producer-conf/direct.conf");
    Config externalConfig = ConfigFactory.load("KafkaUtilsTest/producer-conf/external.conf");
    Properties directProps = KafkaUtils.createProducerProps(directConfig, "test-client-1");
    Properties externalProps = KafkaUtils.createProducerProps(externalConfig, "test-client-1");
    assertThat(directProps.size(), equalTo(externalProps.size()));
    for (Object key : directProps.keySet()) {
      assertThat(String.format("%s should be present in both configs.", key), externalProps.containsKey(key), equalTo(true));
      assertThat(String.format("%s should match.", key), directProps.get(key.toString()).toString(),
          equalTo(externalProps.get(key.toString()).toString()));
    }
  }

  /**
   * The asynchronous publish path relies on producer settings that Lucille never states explicitly,
   * and kafka-clients has changed those defaults underneath it twice (acks 1 -> all in 3.0,
   * linger.ms 0 -> 5 in 4.0), each time in a release upgraded for unrelated reasons. Resolving them
   * through ProducerConfig against the jar actually on the classpath is the only way to know what
   * they are; this test fails on the next such change rather than letting it pass silently.
   */
  @Test
  public void testProducerConfigInvariantsForAsyncPublishing() {
    ProducerConfig resolved = resolve(props("KafkaUtilsTest/producer-conf/direct.conf"));

    // durability: the Publisher stops tracking a document once the producer reports it accepted, so
    // "accepted" has to mean replicated, not merely received by the partition leader
    assertThat(resolved.values().get(ProducerConfig.ACKS_CONFIG), equalTo("-1"));

    // ordering: with several batches in flight per connection, only idempotence keeps a retried
    // batch from landing after a later one. Documents are keyed by id, so this is per-document
    // ordering for repeated ids.
    assertThat(resolved.values().get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG), equalTo(true));

    // latency: pinned by createProducerProps, not inherited. See the comment there.
    assertThat(resolved.values().get(ProducerConfig.LINGER_MS_CONFIG), equalTo(0L));
  }

  /**
   * Throughput settings the producer previously inherited from the client. Left unset they were 16 KB
   * batches, no compression, and no client id to attribute broker-side metrics or quotas to.
   */
  @Test
  public void testProducerThroughputSettingsArePinned() {
    ProducerConfig resolved = resolve(props("KafkaUtilsTest/producer-conf/direct.conf"));

    assertThat(resolved.values().get(ProducerConfig.BATCH_SIZE_CONFIG), equalTo(131072));
    assertThat(resolved.values().get(ProducerConfig.COMPRESSION_TYPE_CONFIG), equalTo("lz4"));
    assertThat(resolved.values().get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG), equalTo(120000));
    assertThat(resolved.values().get(ProducerConfig.CLIENT_ID_CONFIG), equalTo("test-client-1"));
  }

  @Test
  public void testProducerThroughputSettingsAreOverridable() {
    ProducerConfig resolved = resolve(KafkaUtils.createProducerProps(ConfigFactory.parseString(
        "kafka {bootstrapServers: \"localhost:9092\", maxRequestSize: 1048576, "
            + "batchSize: 65536, compressionType: \"zstd\", deliveryTimeoutMs: 300000, lingerMs: 5}"),
        "test-client-1"));

    assertThat(resolved.values().get(ProducerConfig.BATCH_SIZE_CONFIG), equalTo(65536));
    assertThat(resolved.values().get(ProducerConfig.COMPRESSION_TYPE_CONFIG), equalTo("zstd"));
    assertThat(resolved.values().get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG), equalTo(300000));
    assertThat(resolved.values().get(ProducerConfig.LINGER_MS_CONFIG), equalTo(5L));
  }

  /**
   * buffer.memory used to be pinned to maxRequestSize. They bound unrelated things -- one record
   * versus the whole accumulator -- so lowering maxRequestSize to a value a broker would actually
   * accept silently shrank the send buffer along with it.
   */
  @Test
  public void testBufferMemoryIsNotTiedToMaxRequestSize() {
    ProducerConfig resolved = resolve(KafkaUtils.createProducerProps(ConfigFactory.parseString(
        "kafka {bootstrapServers: \"localhost:9092\", maxRequestSize: 1048576}"), "test-client-1"));

    assertThat(resolved.values().get(ProducerConfig.MAX_REQUEST_SIZE_CONFIG), equalTo(1048576));
    assertThat(resolved.values().get(ProducerConfig.BUFFER_MEMORY_CONFIG), equalTo(33554432L));
  }

  @Test
  public void testBufferMemoryIsConfigurable() {
    ProducerConfig resolved = resolve(KafkaUtils.createProducerProps(ConfigFactory.parseString(
        "kafka {bootstrapServers: \"localhost:9092\", maxRequestSize: 1048576, bufferMemory: 67108864}"),
        "test-client-1"));

    assertThat(resolved.values().get(ProducerConfig.BUFFER_MEMORY_CONFIG), equalTo(67108864L));
  }

  /**
   * A record larger than buffer.memory is rejected by the accumulator, so an existing config with an
   * oversized maxRequestSize must not end up with a smaller buffer than it had when the two were
   * aliased.
   */
  @Test
  public void testBufferMemoryDefaultCoversAnOversizedMaxRequestSize() {
    // direct.conf carries the example config's 250 MB maxRequestSize
    ProducerConfig resolved = resolve(props("KafkaUtilsTest/producer-conf/direct.conf"));

    assertThat(resolved.values().get(ProducerConfig.BUFFER_MEMORY_CONFIG), equalTo(250000000L));
  }

  /**
   * A producerPropertyFile replaces Lucille's defaults wholesale, by design. Two things it should
   * not have to restate: the client id, which Lucille assigns per component, and the key serializer,
   * which is not a choice -- every producer keys records by document id.
   */
  @Test
  public void testExternalProducerPropsGetClientIdAndKeySerializer() {
    Properties props = KafkaUtils.createProducerProps(
        ConfigFactory.load("KafkaUtilsTest/producer-conf/external.conf"), "test-client-1");

    assertThat(props.get(ProducerConfig.CLIENT_ID_CONFIG), equalTo("test-client-1"));
    assertThat(props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG),
        equalTo(StringSerializer.class.getName()));
  }

  private static Properties props(String resource) {
    return KafkaUtils.createProducerProps(ConfigFactory.load(resource), "test-client-1");
  }

  /** Resolves through ProducerConfig so defaults come from the kafka-clients jar on the classpath. */
  private static ProducerConfig resolve(Properties props) {
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return new ProducerConfig(props);
  }

  @Test
  public void testCreateEventTopicWhenExists() throws Exception {
    try (MockedStatic<Admin> admin = Mockito.mockStatic(Admin.class)) {
      KafkaFuture future = Mockito.mock(KafkaFuture.class);
      TopicExistsException topicExistsException = new TopicExistsException("test");
      ExecutionException executionException = new ExecutionException(topicExistsException);
      Mockito.doThrow(executionException).when(future).get();

      CreateTopicsResult result = Mockito.mock(CreateTopicsResult.class);
      Mockito.doReturn(future).when(result).all();

      AdminClient adminClient = Mockito.mock(AdminClient.class);
      Mockito.doReturn(result).when(adminClient).createTopics(Mockito.any(), Mockito.any());

      admin.when(() -> Admin.create((Properties)Mockito.any())).thenReturn(adminClient);

      Config directConfig = ConfigFactory.load("KafkaUtilsTest/producer-conf/direct.conf");
      assertFalse(KafkaUtils.createEventTopic(directConfig, "pipeline1", "run1"));
    }
  }

  @Test
  public void testCreateEventTopicWhenDoesNotExist() throws Exception {
    try (MockedStatic<Admin> admin = Mockito.mockStatic(Admin.class)) {
      KafkaFuture future = Mockito.mock(KafkaFuture.class);

      CreateTopicsResult result = Mockito.mock(CreateTopicsResult.class);
      Mockito.doReturn(future).when(result).all();

      AdminClient adminClient = Mockito.mock(AdminClient.class);
      Mockito.doReturn(result).when(adminClient).createTopics(Mockito.any(), Mockito.any());

      admin.when(() -> Admin.create((Properties)Mockito.any())).thenReturn(adminClient);

      Config directConfig = ConfigFactory.load("KafkaUtilsTest/producer-conf/direct.conf");
      assertTrue(KafkaUtils.createEventTopic(directConfig, "pipeline1", "run1"));
    }
  }
}
