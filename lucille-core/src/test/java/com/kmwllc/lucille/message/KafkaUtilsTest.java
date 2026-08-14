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
    Properties directProps = KafkaUtils.createProducerProps(directConfig);
    Properties externalProps = KafkaUtils.createProducerProps(externalConfig);
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
    Config directConfig = ConfigFactory.load("KafkaUtilsTest/producer-conf/direct.conf");
    Properties props = KafkaUtils.createProducerProps(directConfig);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    ProducerConfig resolved = new ProducerConfig(props);

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
