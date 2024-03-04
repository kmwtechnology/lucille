package com.kmwllc.lucille.message;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
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

  @Test
  public void testCreateEventTopicWhenExists() throws Exception {
    try (MockedStatic<Admin> admin = Mockito.mockStatic(Admin.class)) {
      KafkaFuture future = Mockito.mock(KafkaFuture.class);
      TopicExistsException exception = new TopicExistsException("test");
      Mockito.doThrow(exception).when(future).get();

      CreateTopicsResult result = Mockito.mock(CreateTopicsResult.class);
      Mockito.doReturn(future).when(result).all();

      AdminClient adminClient = Mockito.mock(AdminClient.class);
      Mockito.doReturn(result).when(adminClient).createTopics(Mockito.any(), Mockito.any());

      admin.when(() -> Admin.create((Properties)Mockito.any())).thenReturn(adminClient);

;     Config directConfig = ConfigFactory.load("KafkaUtilsTest/producer-conf/direct.conf");
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
