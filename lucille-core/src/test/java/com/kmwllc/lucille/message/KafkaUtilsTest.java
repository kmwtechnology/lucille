package com.kmwllc.lucille.message;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import com.kmwllc.lucille.util.StoringAppender;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
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

    // Idempotence must be enabled on all producers to guarantee per-partition ordering for
    // streaming-mode operations on the same document. Verify it cannot be omitted or overridden.
    assertThat(directProps.get("enable.idempotence").toString(), equalTo("true"));
    assertThat(externalProps.get("enable.idempotence").toString(), equalTo("true"));

    // Verify that a property file explicitly setting enable.idempotence=false is overridden
    Config overrideConfig = ConfigFactory.load("KafkaUtilsTest/producer-conf/external-idempotence-off.conf");
    Properties overrideProps = KafkaUtils.createProducerProps(overrideConfig);
    assertThat(overrideProps.get("enable.idempotence").toString(), equalTo("true"));

    // buffer.memory must be at least 32MB (Kafka's default) regardless of maxRequestSize,
    // so the async producer has room to buffer multiple records
    Config smallMaxRequest = ConfigFactory.parseString("kafka { bootstrapServers: \"localhost:9092\", maxRequestSize: 1048576, maxPollIntervalSecs: 300, consumerGroupId: \"test\" }");
    Properties smallProps = KafkaUtils.createProducerProps(smallMaxRequest);
    assertTrue(Long.parseLong(smallProps.get("buffer.memory").toString()) >= 33_554_432L);

    // buffer.memory grows if maxRequestSize exceeds 32MB
    Config largeMaxRequest = ConfigFactory.parseString("kafka { bootstrapServers: \"localhost:9092\", maxRequestSize: 67108864, maxPollIntervalSecs: 300, consumerGroupId: \"test\" }");
    Properties largeProps = KafkaUtils.createProducerProps(largeMaxRequest);
    assertTrue(Long.parseLong(largeProps.get("buffer.memory").toString()) >= 67_108_864L);
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
  public void testArbitraryProducerPropsWillNotOverwriteIdempotence() {
    Config listPropConfig = ConfigFactory.load("KafkaUtilsTest/producer-conf/idempotence.conf");
    Properties props = KafkaUtils.createProducerProps(listPropConfig);
    // tried to set idempotence to false via arbitrary props, we force it to be true
    assertThat(props.get("enable.idempotence").toString(), equalTo("true"));
  }

  @Test
  public void testInvalidArbitraryProps() {
    // "LIST" is the only invalid type for an arbitrary property... see comment in KafkaUtils
    Config listPropConfig = ConfigFactory.load("KafkaUtilsTest/list-arbitrary.conf");
    assertThrows(IllegalArgumentException.class, () -> KafkaUtils.createProducerProps(listPropConfig));
  }

  private static final String BASE_KAFKA =
      "kafka { bootstrapServers: \"localhost:9092\", maxRequestSize: 1048576, maxPollIntervalSecs: 300, consumerGroupId: \"test\" }\n";

  @Test
  public void testQuotedAndUnquotedDottedKeysProduceSameClientKey() {
    Config quoted = ConfigFactory.parseString(BASE_KAFKA + "kafka.producer { \"linger.ms\": 7 }");
    Config unquoted = ConfigFactory.parseString(BASE_KAFKA + "kafka.producer { linger.ms: 7 }");
    Config nestedObject = ConfigFactory.parseString(BASE_KAFKA + "kafka.producer { linger { ms: 7 } }");

    for (Config config : List.of(quoted, unquoted, nestedObject)) {
      Properties props = KafkaUtils.createProducerProps(config);
      assertEquals(7, props.get("linger.ms"));
      assertFalse(props.containsKey("\"linger.ms\""));
    }
  }

  @Test
  public void testQuotedKeyInsideNestedObject() {
    Config config = ConfigFactory.parseString(BASE_KAFKA + "kafka.producer { ssl { \"keystore.location\": \"/ks\" } }");
    Properties props = KafkaUtils.createProducerProps(config);
    assertEquals("/ks", props.get("ssl.keystore.location"));
    assertEquals(Set.of(), quotedKeys(props));
  }

  @Test
  public void testQuotedKeysForConsumer() {
    Config config = ConfigFactory.parseString(BASE_KAFKA + "kafka.consumer { \"fetch.min.bytes\": 1024 }");
    Properties props = KafkaUtils.createConsumerProps(config, "client-1");
    assertEquals(1024, props.get("fetch.min.bytes"));
    assertEquals(Set.of(), quotedKeys(props));
  }

  @Test
  public void testQuotedKeysForAdmin() throws Exception {
    try (MockedStatic<Admin> admin = Mockito.mockStatic(Admin.class)) {
      CreateTopicsResult result = Mockito.mock(CreateTopicsResult.class);
      Mockito.doReturn(Mockito.mock(KafkaFuture.class)).when(result).all();
      AdminClient adminClient = Mockito.mock(AdminClient.class);
      Mockito.doReturn(result).when(adminClient).createTopics(Mockito.any(), Mockito.any());
      ArgumentCaptor<Properties> captor = ArgumentCaptor.forClass(Properties.class);
      admin.when(() -> Admin.create(captor.capture())).thenReturn(adminClient);

      Config config = ConfigFactory.parseString(BASE_KAFKA + "kafka.admin { \"request.timeout.ms\": 1234 }");
      KafkaUtils.createEventTopic(config, "pipeline1", "run1");

      Properties captured = captor.getValue();
      assertEquals(1234, captured.get("request.timeout.ms"));
      assertEquals(Set.of(), quotedKeys(captured));
    }
  }

  @Test
  public void testConflictingQuotedAndUnquotedKeysThrow() {
    Config config = ConfigFactory.parseString(BASE_KAFKA + "kafka.producer { \"linger.ms\": 0, linger.ms: 5 }");
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> KafkaUtils.createProducerProps(config));
    assertTrue(e.getMessage(), e.getMessage().contains("linger.ms"));
  }

  @Test
  public void testAgreeingQuotedAndUnquotedKeysAreAccepted() {
    Config config = ConfigFactory.parseString(BASE_KAFKA + "kafka.producer { \"linger.ms\": 5, linger.ms: 5 }");
    assertEquals(5, KafkaUtils.createProducerProps(config).get("linger.ms"));
  }

  @Test
  public void testUnknownKeysAreReported() {
    Set<String> known = org.apache.kafka.clients.producer.ProducerConfig.configNames();
    assertEquals(List.of("lingerr.ms", "myString"),
        KafkaUtils.unknownKeys(List.of("linger.ms", "lingerr.ms", "myString", "sasl.jaas.config",
            "ssl.keystore.location", "interceptor.classes"), known));
  }

  @Test
  public void testConfigProviderKeysAreNotReported() {
    Set<String> known = org.apache.kafka.clients.producer.ProducerConfig.configNames();
    assertEquals(List.of(), KafkaUtils.unknownKeys(List.of("config.providers", "config.providers.file.class",
        "config.providers.file.param.path"), known));
  }

  @Test
  public void testSubstitutedStringAndLiteralNumberAreNotAConflict() {
    Config config = ConfigFactory.parseString(BASE_KAFKA
        + "kafka.lingerFromEnv: \"5\"\nkafka.producer { \"linger.ms\": ${kafka.lingerFromEnv}, linger.ms: 5 }").resolve();
    assertEquals("5", String.valueOf(KafkaUtils.createProducerProps(config).get("linger.ms")));
  }

  @Test
  public void testUnknownKeyWarningUsesTheRightClientAndIsLoggedOnce() {
    List<String> warnings = new CopyOnWriteArrayList<>();
    StoringAppender appender = new StoringAppender() {
      @Override
      public void append(LogEvent event) {
        if (event.getLevel() == Level.WARN) {
          warnings.add(event.getMessage().getFormattedMessage());
        }
      }
    };
    appender.start();
    Logger logger = (Logger) LogManager.getLogger(KafkaUtils.class);
    logger.addAppender(appender);
    try {
      // transactional.id is a producer-only key: unknown to the consumer, known to the producer
      Config config = ConfigFactory.parseString(BASE_KAFKA
          + "kafka.consumer { \"transactional.id\": \"t\" }\nkafka.producer { \"transactional.id\": \"t\" }");
      KafkaUtils.createConsumerProps(config, "client-1");
      KafkaUtils.createConsumerProps(config, "client-2");
      KafkaUtils.createProducerProps(config);

      assertEquals(warnings.toString(), 1, warnings.size());
      assertTrue(warnings.get(0), warnings.get(0).startsWith("kafka.consumer.transactional.id "));
    } finally {
      logger.removeAppender(appender);
      appender.stop();
    }
  }

  private static Set<String> quotedKeys(Properties props) {
    return props.stringPropertyNames().stream().filter(k -> k.contains("\"")).collect(Collectors.toSet());
  }

  @Test
  public void testDocumentConsumerRejectsAutoCommit() {
    // The indexer commits destination-topic offsets on batch completion; auto-commit would commit them at poll time
    // (before indexing) and could skip documents on a crash. An override that re-enables it must fail fast rather than
    // silently undermine the design.
    Config autoCommitOn = ConfigFactory.parseString(
        "kafka { bootstrapServers: \"localhost:9092\", maxPollIntervalSecs: 300, consumerGroupId: \"test\", "
        + "consumer { enable.auto.commit: true } }");
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> KafkaUtils.createDocumentConsumer(autoCommitOn, "test-client"));
    assertTrue(e.getMessage().contains("enable.auto.commit"));
  }

  @Test
  public void testDocumentConsumerRejectsUnsetAutoCommitFromPropertyFile() {
    // kafka-clients defaults enable.auto.commit to true, so a consumerPropertyFile that OMITS it would auto-commit at
    // poll just as surely as one that sets it true. An unset value must be rejected, not treated as safe.
    Config unsetInFile = ConfigFactory.load("KafkaUtilsTest/consumer-conf/external-no-auto-commit-setting.conf");
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> KafkaUtils.createDocumentConsumer(unsetInFile, "test-client"));
    assertTrue(e.getMessage().contains("enable.auto.commit"));
  }

  @Test
  public void testDocumentConsumerAllowsExplicitlyDisabledAutoCommit() {
    // Explicitly setting it to the required value must be accepted (constructing the consumer connects lazily, so this
    // does not require a running broker).
    Config autoCommitOff = ConfigFactory.parseString(
        "kafka { bootstrapServers: \"localhost:9092\", maxPollIntervalSecs: 300, consumerGroupId: \"test\", "
        + "consumer { enable.auto.commit: false } }");
    KafkaUtils.createDocumentConsumer(autoCommitOff, "test-client").close();
  }

  @Test
  public void testDocumentConsumerAllowsAutoCommitDisabledInPropertyFile() {
    // A consumerPropertyFile that explicitly sets enable.auto.commit=false is accepted.
    Config disabledInFile = ConfigFactory.load("KafkaUtilsTest/consumer-conf/external.conf");
    KafkaUtils.createDocumentConsumer(disabledInFile, "test-client").close();
  }

  @Test
  public void testGetPollInterval() {
    assertEquals(Duration.ofMillis(2000), KafkaUtils.getPollInterval(ConfigFactory.empty()));
    assertEquals(Duration.ofMillis(150), KafkaUtils.getPollInterval(ConfigFactory.parseString("kafka.pollIntervalMs: 150")));
    assertThrows(IllegalArgumentException.class,
        () -> KafkaUtils.getPollInterval(ConfigFactory.parseString("kafka.pollIntervalMs: 0")));
  }
}
