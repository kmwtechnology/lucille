package com.kmwllc.lucille.message;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.Test;

import java.util.Properties;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
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
  public void testArbitraryAdminProps() throws Exception {
    try (MockedStatic<Admin> admin = Mockito.mockStatic(Admin.class)) {
      KafkaFuture future = Mockito.mock(KafkaFuture.class);
      TopicExistsException topicExistsException = new TopicExistsException("test");
      ExecutionException executionException = new ExecutionException(topicExistsException);
      Mockito.doThrow(executionException).when(future).get();

      CreateTopicsResult result = Mockito.mock(CreateTopicsResult.class);
      Mockito.doReturn(future).when(result).all();

      AdminClient adminClient = Mockito.mock(AdminClient.class);
      Mockito.doReturn(result).when(adminClient).createTopics(Mockito.any(), Mockito.any());

      ArgumentCaptor<Properties> captor = ArgumentCaptor.forClass(Properties.class);
      admin.when(() -> Admin.create(captor.capture())).thenReturn(adminClient);

      Config directConfig = ConfigFactory.load("KafkaUtilsTest/admin-conf/direct.conf");
      KafkaUtils.createEventTopic(directConfig, "pipeline1", "run1");

      Properties captured = captor.getValue();
      assertEquals("localhost:1234", captured.getProperty("bootstrap.servers"));
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

  @Test
  public void testCreateConsumerPropsArbitrary() {
    Config directConfig = ConfigFactory.load("KafkaUtilsTest/consumer-conf/direct-arbitrary.conf");
    Config externalConfig = ConfigFactory.load("KafkaUtilsTest/consumer-conf/external-arbitrary.conf");
    Properties directProps = KafkaUtils.createConsumerProps(directConfig, "test-client-1");
    Properties externalProps = KafkaUtils.createConsumerProps(externalConfig, "test-client-1");

    assertThat(directProps.size(), equalTo(externalProps.size()));
    for (Object key : directProps.keySet()) {
      // we overwrite this in external-arbitrary to ensure property file values get overwritten
      if (key == "bootstrap.servers") {
        continue;
      }

      assertThat(String.format("%s should be present in both configs.", key), externalProps.containsKey(key), equalTo(true));
      assertThat(String.format("%s should match.", key), directProps.get(key.toString()).toString(),
          equalTo(externalProps.get(key.toString()).toString()));
    }

    // checking values, but also types, from our arbitrary props
    assertEquals("string", directProps.get("myString"));
    assertEquals(true, directProps.get("myBoolean"));
    assertEquals(123, directProps.get("myNumber"));

    assertEquals("string", externalProps.get("myString"));
    assertEquals(true, externalProps.get("myBoolean"));
    assertEquals(123, externalProps.get("myNumber"));

    // also make sure that the properties overwrite from the file
    // file has it specified as localhost:9092
    assertEquals("localhost:1234", externalProps.get("bootstrap.servers"));
  }

  @Test
  public void testCreateProducerPropsArbitrary() {
    Config directConfig = ConfigFactory.load("KafkaUtilsTest/producer-conf/direct-arbitrary.conf");
    Config externalConfig = ConfigFactory.load("KafkaUtilsTest/producer-conf/external-arbitrary.conf");

    Properties directProps = KafkaUtils.createProducerProps(directConfig);
    Properties externalProps = KafkaUtils.createProducerProps(externalConfig);

    // less testing (types/values etc.) given the previous test.
    assertEquals("string", directProps.get("myString"));
    // see the .conf file for an important comment on object notation, if desired...
    assertEquals("none", externalProps.get("security.protocol"));
  }

  @Test
  public void testInvalidArbitraryProps() {
    // only lists are invalid
    Config listPropConfig = ConfigFactory.load("KafkaUtilsTest/list-arbitrary.conf");
    assertThrows(IllegalArgumentException.class, () -> KafkaUtils.createProducerProps(listPropConfig));
  }
}
