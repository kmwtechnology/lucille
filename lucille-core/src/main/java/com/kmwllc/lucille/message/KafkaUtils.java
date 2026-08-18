package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.util.FileContentFetcher;
import com.kmwllc.lucille.core.KafkaDocument;
import com.typesafe.config.Config;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for interacting with Kafka: creating Kafka clients, determining Kafka topic names, etc.
 *  TODO: add config option for cleaning up kafka topics at end of run
 *  prepend run id to each topic name
 *  add separate config option to leave failure topic around
 *  consider adding all failed documents to the failure topic
 *  add ability to override names of any kafka topic in the config
 *
 *  s3 (Map, Optional) : If your property files are held in S3. See FileConnector for the appropriate arguments to provide.
 *  azure (Map, Optional) : If your property files are held in Azure. See FileConnector for the appropriate arguments to provide.
 *  gcp (Map, Optional) : If your property files are held in Google Cloud. See FileConnector for the appropriate arguments to provide.
 */
public class KafkaUtils {

  public static Spec SPEC = SpecBuilder.withoutDefaults()
      .requiredString("bootstrapServers", "consumerGroupId")
      .requiredNumber("maxPollIntervalSecs", "maxRequestSize")
      .optionalString("documentSerializer", "documentDeserializer", "events", "consumerPropertyFile",
          "producerPropertyFile", "adminPropertyFile", "securityProtocol", "sourceTopic", "eventTopic",
          "compressionType")
      .optionalNumber("metadataMaxAgeMs", "lingerMs", "bufferMemory", "batchSize", "deliveryTimeoutMs").build();

  public static final Duration POLL_INTERVAL = Duration.ofMillis(2000);
  private static final Logger log = LoggerFactory.getLogger(KafkaUtils.class);

  /** kafka-clients' own buffer.memory default, used as the floor when Lucille sizes the accumulator. */
  private static final long DEFAULT_BUFFER_MEMORY = 33_554_432L;
  private static final int DEFAULT_BATCH_SIZE = 131_072;
  private static final String DEFAULT_COMPRESSION_TYPE = "lz4";
  private static final int DEFAULT_DELIVERY_TIMEOUT_MS = 120_000;

  private static Properties loadExternalProps(String filename, Config config) {
    try (Reader propertiesReader = FileContentFetcher.getOneTimeReader(filename, StandardCharsets.UTF_8.name(), config)) {
      Properties consumerProps = new Properties();
      consumerProps.load(propertiesReader);
      return consumerProps;
    } catch (IOException e) {
      throw new IllegalStateException(String.format("Cannot load kafka property file %s.", filename));
    }
  }

  //access set to public so KafkaConnector can create consumer props from config.
  // unit tests can also validate created properties without initializing a Consumer
  public static Properties createConsumerProps(Config config, String clientId) {
    if (config.hasPath("kafka.consumerPropertyFile")) {
      Properties loadedProps = loadExternalProps(config.getString("kafka.consumerPropertyFile"), config);
      loadedProps.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
      return loadedProps;
    }

    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrapServers"));
    if (config.hasPath("kafka.securityProtocol")) {
      consumerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, config.getString("kafka.securityProtocol"));
    }
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, config.getString("kafka.consumerGroupId"));
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1000 * config.getInt("kafka.maxPollIntervalSecs"));
    consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, ConfigUtils.getOrDefault(config, "kafka.metadataMaxAgeMs", 30000));
    return consumerProps;
  }

  /**
   * Creates a Kafka consumer for documents. This consumer will not commit offsets automatically. The consumer should commit offsets after the
   * document(s) returned by the poll has been processed.
   * @param config - A config object containing Kafka properties. These will be used to configure the consumer. The provided config object can be used to configure the following kafka consumer settings:
   *               - kafka.bootstrapServers - The bootstrap servers to use for connecting to Kafka.
   *               - kafka.securityProtocol - The security protocol to use when connecting to Kafka.
   *               - kafka.consumerGroupId - The consumer group id to use when subscribing to the event topic.
   *               - kafka.maxPollIntervalSecs - The maximum amount of time in seconds that the consumer will wait when polling for new messages from Kafka.
   *               - kafka.metadataMaxAgeMs - The maximum amount of time in milliseconds that the consumer will cache metadata about topics and partitions..
   *               - kafka.documentDeserializer - The deserializer class to use when deserializing documents. Defaults to KafkaDocumentDeserializer.
   * @param clientId - The id that will be used by the consumer as the client id when communicating with Kafka.
   * @return A Kafka consumer for events. The provided Consumer will *not* be configured to commit offsets automatically. The caller is responsible for committing offsets after processing documents.
   */
  public static KafkaConsumer<String, KafkaDocument> createDocumentConsumer(Config config, String clientId) {
    Properties consumerProps = createConsumerProps(config, clientId);
    String deserializerClass = config.hasPath("kafka.documentDeserializer")
        ? config.getString("kafka.documentDeserializer")
        : KafkaDocumentDeserializer.class.getName();
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializerClass);
    return new KafkaConsumer<>(consumerProps);
  }

  /**
   * Creates a Kafka consumer for events. The created consumer will be configured to commit offsets automatically. This is to avoid
   * the impact on throughput from synchronously committing offsets for every event processed or the impact on kafka resources from commits stacking on top of each
   * other from asynchronsly committing offsets for every event processed.
   *
   *
   * @param config - A config object containing Kafka properties. These will be used to configure the consumer. The provided config object can be used to configure the following kafka consumer settings:
   *               - kafka.bootstrapServers - The bootstrap servers to use for connecting to Kafka.
   *               - kafka.securityProtocol - The security protocol to use when connecting to Kafka.
   *               - kafka.consumerGroupId - The consumer group id to use when subscribing to the event topic.
   *               - kafka.maxPollIntervalSecs - The maximum amount of time in seconds that the consumer will wait when polling for new messages from Kafka.
   *               - kafka.metadataMaxAgeMs - The maximum amount of time in milliseconds that the consumer will cache metadata about topics and partitions..
   * @param clientId - The id that will be used by the consumer as the client id when communicating with Kafka.
   * @return A Kafka consumer for events. The provided Consumer will *always* be configured to commit offsets automatically.
   */
  public static KafkaConsumer<String, String> createEventConsumer(Config config, String clientId) {
    Properties consumerProps = createConsumerProps(config, clientId);

    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    return new KafkaConsumer<>(consumerProps);
  }

  //access set to package so unit tests can validate created properties without initializing a Producer
  static Properties createProducerProps(Config config, String clientId) {
    if (config.hasPath("kafka.producerPropertyFile")) {
      Properties loadedProps = loadExternalProps(config.getString("kafka.producerPropertyFile"), config);
      // the file replaces Lucille's settings wholesale, by design -- but the client id is assigned per
      // component, and every producer keys its records by document id, so neither is the file's to pick
      loadedProps.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
      loadedProps.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      return loadedProps;
    }
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrapServers"));
    if (config.hasPath("kafka.securityProtocol")) {
      producerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, config.getString("kafka.securityProtocol"));
    }
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    // without a client id the broker cannot tell Lucille's producers apart in its request logs, its
    // metrics, or its quotas -- every one of them reports as the empty default
    producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);

    int maxRequestSize = config.getInt("kafka.maxRequestSize");
    producerProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, maxRequestSize);
    // buffer.memory used to be pinned to maxRequestSize. They bound unrelated things -- the whole
    // accumulator versus a single record -- so bringing maxRequestSize down to a size a broker will
    // actually accept silently shrank the send buffer with it. The default still covers an oversized
    // maxRequestSize, because a record larger than the accumulator can never be sent at all.
    producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.hasPath("kafka.bufferMemory")
        ? config.getLong("kafka.bufferMemory")
        : Math.max(DEFAULT_BUFFER_MEMORY, maxRequestSize));

    // Pinned rather than left to the client default, which changed from 0 to 5 in kafka-clients 4.0.
    // The publisher's send is asynchronous, so its threads do not wait on a linger and batches fill
    // from the queue depth instead (91 records/request measured at linger 0). Everything else here --
    // the worker's document sends, every event send -- is still send().get(), where any non-zero
    // linger is added directly to the per-record latency. Revisit when those paths go async.
    producerProps.put(ProducerConfig.LINGER_MS_CONFIG,
        config.hasPath("kafka.lingerMs") ? config.getLong("kafka.lingerMs") : 0L);
    // the client default of 16 KB is small for Lucille documents now that batches actually fill
    producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG,
        config.hasPath("kafka.batchSize") ? config.getInt("kafka.batchSize") : DEFAULT_BATCH_SIZE);
    // documents go over the wire as JSON, which compresses well; the cost is per batch, so this only
    // became worth paying once the sends stopped being one record at a time
    producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
        config.hasPath("kafka.compressionType") ? config.getString("kafka.compressionType") : DEFAULT_COMPRESSION_TYPE);
    // states the client default as a deliberate bound rather than inheriting it
    producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
        config.hasPath("kafka.deliveryTimeoutMs") ? config.getInt("kafka.deliveryTimeoutMs") : DEFAULT_DELIVERY_TIMEOUT_MS);

    // Not configurable: the Publisher stops tracking a document as soon as the producer reports it
    // accepted, so "accepted" has to mean replicated, and a retried batch must not land after a later
    // one. Both of these were inherited defaults that changed underneath Lucille in kafka-clients 3.0.
    // A deployment that genuinely needs different values can supply a kafka.producerPropertyFile.
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
    producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    return producerProps;
  }

  public static KafkaProducer<String, Document> createDocumentProducer(Config config, String clientId) {
    Properties producerProps = createProducerProps(config, clientId);
    String serializerClass = config.hasPath("kafka.documentSerializer")
        ? config.getString("kafka.documentSerializer")
        : KafkaDocumentSerializer.class.getName();
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializerClass);
    return new KafkaProducer<>(producerProps);
  }

  public static KafkaProducer<String, String> createEventProducer(Config config, String clientId) {
    if (config.hasPath("kafka.events") && !config.getBoolean("kafka.events")) {
      return null;
    }
    Properties producerProps = createProducerProps(config, clientId);
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return new KafkaProducer<>(producerProps);
  }

  public static String getEventTopicName(Config config, String pipelineName, String runId) {
    if (config.hasPath("kafka.eventTopic")) {
      return config.getString("kafka.eventTopic");
    } else {
      return pipelineName + "_event_" + runId;
    }
  }

  public static String getSourceTopicName(String pipelineName, Config config) {
    String topicName;
    if (config.hasPath("kafka.sourceTopic")) {
      topicName = config.getString("kafka.sourceTopic");
    } else {
      // sanitize pipelineName because it may be used as part of the Kafka topic name passed through as a Pattern
      if (pipelineName == null) {
        throw new IllegalArgumentException("Pipeline name cannot be null when using it for a kafka topic name.");
      }
      if (pipelineName.matches("^[A-Za-z\\d\\._\\-]+$")) {
        topicName = pipelineName + "_source";
      } else {
        throw new IllegalArgumentException("Invalid characters in pipelineName: " + pipelineName);
      }

    }

    // make sure topicName does not exceed 249 characters, also limited by kafka
    if (topicName != null && topicName.length() > 249) {
      throw new IllegalArgumentException("Invalid topic name because it is too long (max 249 characters): " + topicName);
    }

    return topicName;
  }

  public static String getDestTopicName(String pipelineName) {
    return pipelineName + "_dest";
  }

  public static String getFailTopicName(String pipelineName) {
    return pipelineName + "_fail";
  }

  public static void validateAtMostOneRecord(ConsumerRecords<?, ?> consumerRecords) throws Exception {
    if (consumerRecords.count() > 1) {
      throw new Exception("Kafka poll returned more than 1 message but this shouldn't happen");
    }
  }

  /**
   * Creates an event topic for the designated pipeline and runId.
   *
   * @return true if the topic was created; false if the topic already existed, so no action was taken;
   * any problem encountered in creating the topic, other than the topic already existing, will result
   * in an exception, not a false return value
   */
  public static boolean createEventTopic(Config config, String pipelineName, String runId)
      throws ExecutionException, InterruptedException {
    String eventTopicName = KafkaUtils.getEventTopicName(config, pipelineName, runId);

    // create the event topic explicitly so we can guarantee that it will have 1 partition
    // multiple partitions could lead to events being received out of order which
    // could mean that child creation events arrive after parent completion events, which could
    // interfere with the publisher's logic for determining when the run is complete

    Properties props;
    if (config.hasPath("kafka.adminPropertyFile")) {
      props = loadExternalProps(config.getString("kafka.adminPropertyFile"), config);
    } else {
      props = new Properties();
      props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrapServers"));
    }

    try (Admin kafkaAdminClient = Admin.create(props)) {
      NewTopic eventTopic = new NewTopic(eventTopicName, 1, (short) 1);
      CreateTopicsResult result = kafkaAdminClient.createTopics(List.of(eventTopic), new CreateTopicsOptions());
      KafkaFuture<Void> future = result.all();
      future.get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof TopicExistsException) {
        log.warn("Event topic {} already exists.", eventTopicName);
        return false;
      }
      throw e;
    }

    return true;
  }
}
