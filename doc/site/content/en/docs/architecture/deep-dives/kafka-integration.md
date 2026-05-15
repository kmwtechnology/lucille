---
title: Kafka Integration
weight: 8
date: 2025-06-09
description: >
  Topic naming, KafkaDocument metadata, serialization, consumer groups, offset strategies, and configuration.
---

## Overview

Kafka is Lucille's distributed messaging backbone. When running in `KAFKA_LOCAL` or `KAFKA_DISTRIBUTED` mode, all inter-component communication flows through Kafka topics. This enables horizontal scaling: multiple Worker processes can consume from the same source topic, and multiple Indexer processes can consume from the same dest topic.

## Topic Naming Conventions

Lucille uses four topics per pipeline, named by convention:

```java
public static String getSourceTopicName(String pipelineName, Config config) {
    // Override: kafka.sourceTopic
    // Default: {pipelineName}_source
    return pipelineName + "_source";
}

public static String getDestTopicName(String pipelineName) {
    return pipelineName + "_dest";
}

public static String getFailTopicName(String pipelineName) {
    return pipelineName + "_fail";
}

public static String getEventTopicName(Config config, String pipelineName, String runId) {
    // Override: kafka.eventTopic
    // Default: {pipelineName}_event_{runId}
    return pipelineName + "_event_" + runId;
}
```

| Topic | Purpose | Producers | Consumers |
|-------|---------|-----------|-----------|
| `{pipeline}_source` | Documents waiting to be processed | Publisher | Workers |
| `{pipeline}_dest` | Processed documents waiting to be indexed | Workers | Indexers |
| `{pipeline}_fail` | Poison-pill documents (dead letter queue) | Workers | External monitoring |
| `{pipeline}_event_{runId}` | Lifecycle events back to Publisher | Workers, Indexers | Publisher |

The event topic is **per-run** (includes the runId) because each run needs its own isolated event stream. The source, dest, and fail topics are per-pipeline and persist across runs.

The source topic name is validated to contain only safe characters (`[A-Za-z\d._-]+`) since it may be used as a regex pattern for consumer subscription.

## KafkaDocument: Carrying Kafka Metadata

`KafkaDocument` extends `JsonDocument` to carry partition/offset/key metadata alongside document fields:

```java
public class KafkaDocument extends JsonDocument {
    private String topic;
    private int partition;
    private long offset;
    private String key;

    public void setKafkaMetadata(ConsumerRecord<String, ?> record) {
        this.topic = record.topic();
        this.partition = record.partition();
        this.offset = record.offset();
        this.key = record.key();
    }
}
```

This metadata travels with the document through the pipeline. It's essential for the Hybrid mode where the Indexer needs to report back which offsets have been successfully processed.

Plain `Document` objects are written to Kafka. When deserialized, they come back as `KafkaDocument` instances with the Kafka metadata attached from the `ConsumerRecord`.

## Serializer/Deserializer

Documents are serialized as JSON using Jackson:

```java
public class KafkaDocumentSerializer implements Serializer<Document> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, Document doc) {
        if (doc == null) return null;
        return MAPPER.writeValueAsBytes(doc);
    }
}

public class KafkaDocumentDeserializer implements Deserializer<Document> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public Document deserialize(String topic, byte[] data) {
        if (data == null) return null;
        return new KafkaDocument((ObjectNode) MAPPER.readTree(data));
    }
}
```

The deserializer always produces a `KafkaDocument` (even though the return type is `Document`). The Kafka metadata is set separately after deserialization via `setKafkaMetadata()`.

Custom serializers/deserializers can be specified via config:
```hocon
kafka.documentSerializer = "com.example.MySerializer"
kafka.documentDeserializer = "com.example.MyDeserializer"
```

## Document ID as Kafka Message Key

Documents are produced with their ID as the Kafka key:

```java
// In KafkaPublisherMessenger:
kafkaProducer.send(new ProducerRecord(sourceTopicName, document.getId(), document));

// In KafkaWorkerMessenger:
kafkaDocumentProducer.send(new ProducerRecord<>(destTopicName, document.getId(), document));
```

This provides **ordering guarantees**: all messages with the same key go to the same partition, ensuring that a document and its children are processed in order within a single partition. It also means that if the same document ID is published multiple times, all versions land on the same partition.

## Consumer Group Management

Workers and Indexers join consumer groups to enable parallel consumption:

```java
consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, config.getString("kafka.consumerGroupId"));
```

All Workers for a pipeline share the same consumer group. Kafka distributes partitions among group members, so adding more Workers increases parallelism (up to the number of partitions).

Each consumer gets a unique client ID to avoid Kafka warnings:
```java
String kafkaClientId = "com.kmwllc.lucille-worker-" + pipelineName + "-" + RandomStringUtils.randomAlphanumeric(8);
```

Key consumer settings:
```java
consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);          // One doc at a time
consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");  // Manual commits
```

## The `maxPollIntervalSecs` Setting

```java
consumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 
    1000 * config.getInt("kafka.maxPollIntervalSecs"));
```

This is the maximum time between `poll()` calls before Kafka considers the consumer dead and triggers a rebalance. It must be set higher than the longest expected document processing time. If a document takes longer to process than this interval, the consumer will be kicked from the group and the document will be reprocessed by another consumer.

## Topic Creation

The event topic is created explicitly with exactly **one partition**:

```java
public static boolean createEventTopic(Config config, String pipelineName, String runId) {
    String eventTopicName = KafkaUtils.getEventTopicName(config, pipelineName, runId);

    // Single partition is critical for ordering
    NewTopic eventTopic = new NewTopic(eventTopicName, 1, (short) 1);

    try (Admin kafkaAdminClient = Admin.create(props)) {
        CreateTopicsResult result = kafkaAdminClient.createTopics(List.of(eventTopic));
        result.all().get();
    } catch (ExecutionException e) {
        if (e.getCause() instanceof TopicExistsException) {
            return false;  // Already exists, that's fine
        }
        throw e;
    }
    return true;
}
```

**Why single partition for events?** Multiple partitions could cause events to arrive out of order. If a child's FINISH event arrives before its CREATE event (because they're on different partitions), the Publisher's accounting logic would be corrupted. A single partition guarantees FIFO ordering.

The source and dest topics are NOT explicitly created by Lucille — they're expected to exist already or be auto-created by Kafka's broker configuration.

## The Event Topic: Lifecycle Events

Events flow from Workers and Indexers back to the Publisher:

- **Worker → Event Topic**: `CREATE` (child document generated), `FAIL` (processing error), `DROP` (document dropped by stage)
- **Indexer → Event Topic**: `FINISH` (successfully indexed), `FAIL` (indexing error)

Events are serialized as JSON strings (not using the document serializer):

```java
// Producer uses StringSerializer for events
producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

// Sending an event
kafkaEventProducer.send(
    new ProducerRecord<>(confirmationTopicName, event.getDocumentId(), event.toString()));
```

The event consumer uses auto-commit for throughput:
```java
consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
```

This is acceptable because event loss is not catastrophic — the worst case is the Publisher waits longer or times out. Duplicate events are handled gracefully by the Publisher's Bag-based accounting.

Events can be disabled entirely:
```hocon
kafka.events = false
```

When disabled, `createEventProducer()` returns null and all `sendEvent()` calls become no-ops. This is useful for Workers/Indexers that run independently without a Publisher waiting for completion.

## The Fail Topic (Dead Letter Queue)

Documents that exceed retry limits are sent to the fail topic:

```java
// In KafkaWorkerMessenger:
public void sendFailed(Document document) throws Exception {
    ProducerRecord<String, Document> producerRecord =
        new ProducerRecord<>(KafkaUtils.getFailTopicName(pipelineName), document.getId(), document);
    kafkaDocumentProducer.send(producerRecord).get();
    kafkaDocumentProducer.flush();
}
```

The fail topic (`{pipeline}_fail`) acts as a dead letter queue. Documents here can be inspected, fixed, and replayed. The Worker sends a document to the fail topic when its retry count (tracked in ZooKeeper) exceeds `worker.maxRetries`.

## Kafka Configuration Options

All Kafka settings live under the `kafka` config prefix:

| Config Key | Purpose | Required |
|-----------|---------|----------|
| `kafka.bootstrapServers` | Kafka broker addresses | Yes (unless using property files) |
| `kafka.securityProtocol` | Security protocol (PLAINTEXT, SSL, SASL_SSL, etc.) | No |
| `kafka.consumerGroupId` | Consumer group for Workers/Indexers | Yes |
| `kafka.maxPollIntervalSecs` | Max time between polls before rebalance | Yes |
| `kafka.maxRequestSize` | Max message size in bytes | Yes |
| `kafka.metadataMaxAgeMs` | Metadata cache TTL | No (default: 30000) |
| `kafka.sourceTopic` | Override source topic name | No |
| `kafka.eventTopic` | Override event topic name | No |
| `kafka.events` | Enable/disable event production | No (default: true) |
| `kafka.documentSerializer` | Custom serializer class | No |
| `kafka.documentDeserializer` | Custom deserializer class | No |
| `kafka.consumerPropertyFile` | Path to external consumer properties | No |
| `kafka.producerPropertyFile` | Path to external producer properties | No |
| `kafka.adminPropertyFile` | Path to external admin properties | No |

## Partitions and Parallelism

The relationship between Kafka partitions and Lucille parallelism:

- **Source topic partitions** determine max Worker parallelism. With 8 partitions, at most 8 Workers can consume concurrently (within the same consumer group).
- **Dest topic partitions** determine max Indexer parallelism. Same principle.
- **Event topic** always has 1 partition (ordering requirement).

To scale Workers: increase source topic partitions and add more Worker processes/threads.

Lucille polls one record at a time (`MAX_POLL_RECORDS_CONFIG = 1`) to ensure fine-grained offset control and prevent one slow document from blocking a batch.

## External Property Files for Advanced Configuration

For complex Kafka setups (SASL, SSL, custom partitioners), external property files can be specified:

```hocon
kafka.consumerPropertyFile = "/path/to/consumer.properties"
kafka.producerPropertyFile = "/path/to/producer.properties"
kafka.adminPropertyFile = "/path/to/admin.properties"
```

When a property file is specified, it completely replaces the programmatic configuration (except for `CLIENT_ID_CONFIG` which is always set). The file is loaded via `FileContentFetcher` which supports local files and cloud storage (S3, Azure, GCP).

```java
private static Properties loadExternalProps(String filename, Config config) {
    try (Reader propertiesReader = FileContentFetcher.getOneTimeReader(filename, StandardCharsets.UTF_8.name(), config)) {
        Properties consumerProps = new Properties();
        consumerProps.load(propertiesReader);
        return consumerProps;
    }
}
```

## Offset Commit Strategies

Different components use different commit strategies:

| Component | Strategy | Rationale |
|-----------|----------|-----------|
| Worker (source) | `commitSync()` after processing | Minimize reprocessing after crash |
| Indexer (dest) | `commitSync()` immediately after poll | Acceptable because indexing is idempotent |
| Publisher (events) | Auto-commit | Throughput; duplicate events are harmless |
| Hybrid Worker | Deferred commit via offset queue | Only commit after Indexer confirms |

The Worker's synchronous commit ensures that if a Worker crashes, the document it was processing will be redelivered to another Worker. Without this, the document could be lost (committed but not processed).

## Producer Behavior

All producers use synchronous sends (`.get()` after `send()`):

```java
kafkaDocumentProducer.send(new ProducerRecord<>(...)).get();
kafkaDocumentProducer.flush();
```

This ensures the message is acknowledged by the broker before proceeding. Combined with `MAX_POLL_RECORDS_CONFIG = 1`, this creates a strict one-at-a-time processing model that prioritizes correctness over throughput.

Producer settings:
```java
producerProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, config.getInt("kafka.maxRequestSize"));
producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.getInt("kafka.maxRequestSize"));
```

Both `maxRequestSize` and `bufferMemory` are set to the same value, ensuring the producer can always send a single maximum-sized message.
