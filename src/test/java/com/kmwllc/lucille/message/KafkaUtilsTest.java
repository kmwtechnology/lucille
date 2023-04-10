package com.kmwllc.lucille.message;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class KafkaUtilsTest {

  @Test
  public void testCreateConsumerProps() {
    Config directConfig = ConfigFactory.load("KafkaUtilsTest/consumer-conf/direct.conf");
    Config externalConfig = ConfigFactory.load("KafkaUtilsTest/consumer-conf/external.conf");
    Properties directProps = KafkaUtils.createConsumerProps(directConfig, "test-client-1");
    Properties externalProps = KafkaUtils.createConsumerProps(externalConfig, "test-client-1");
    assertThat(directProps.size(), equalTo(externalProps.size()));
    for(Object key : directProps.keySet()) {
      assertThat(String.format("%s should be present in both configs.", key), externalProps.containsKey(key), equalTo(true));
      assertThat(String.format("%s should match.", key), directProps.get(key.toString()).toString(), equalTo(externalProps.get(key.toString()).toString()));
    }
  }

  @Test
  public void testCreateProducerProps() {
    Config directConfig = ConfigFactory.load("KafkaUtilsTest/producer-conf/direct.conf");
    Config externalConfig = ConfigFactory.load("KafkaUtilsTest/producer-conf/external.conf");
    Properties directProps = KafkaUtils.createProducerProps(directConfig);
    Properties externalProps = KafkaUtils.createProducerProps(externalConfig);
    assertThat(directProps.size(), equalTo(externalProps.size()));
    for(Object key : directProps.keySet()) {
      assertThat(String.format("%s should be present in both configs.", key),externalProps.containsKey(key), equalTo(true));
      assertThat(String.format("%s should match.", key), directProps.get(key.toString()).toString(), equalTo(externalProps.get(key.toString()).toString()));
    }
  }
}
