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

### Per-Run Event Topics in Batch Mode

In batch mode, each run gets its own event topic (e.g., `my_pipeline_event_c1d9413a-8191-4f4a-92bb-0fc42b5499e3`). Lucille creates this topic via the Kafka Admin API at the start of each run. This means:

- **Kafka Admin API access is required.** Lucille creates the event topic explicitly via the Admin API to guarantee it has exactly 1 partition (required for FIFO event ordering). Kafka's `auto.create.topics.enable` is NOT sufficient — auto-created topics use the broker's default partition count, which would create a multi-partition topic and break the Publisher's accounting logic. If your Kafka cluster requires separate admin credentials, provide them via `kafka.adminPropertyFile`.
- **Each batch ingest produces a new topic.** Over time, event topics accumulate. Consider a retention policy or periodic cleanup of old event topics.
- **Each Publisher requires its own dedicated event topic.** There is no support for multiple Publishers sharing a single event topic and filtering events by run_id. The per-run topic IS the isolation mechanism.

**Do NOT set `kafka.eventTopic` in batch mode.** Setting a fixed event topic name would cause all runs to share one topic. If two runs overlap (e.g., launched via the RunnerManager API), their events would be interleaved on the same topic. Each Publisher would see events from the other run, corrupting its accounting and potentially declaring completion prematurely.

`kafka.eventTopic` is safe only in streaming mode, where there is no Publisher performing completion accounting.

This topic-creation requirement does not apply in streaming mode — when events are disabled (`kafka.events: false`) no event topic is needed, and when a fixed `kafka.eventTopic` is set the topic can be pre-created once and reused indefinitely.

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

### Event Topic Naming in Streaming Mode

In streaming mode (no Runner, no Publisher), documents arrive from an external system without a `run_id` field. Since the default event topic name is `{pipeline}_event_{runId}`, a null run_id produces a topic named `{pipeline}_event_null`. This is functional but awkward.

To avoid this, use `kafka.eventTopic` to set a fixed topic name:

```hocon
kafka {
  events: true
  eventTopic: "lucille_events"  # fixed name, independent of run_id
}
```

When `kafka.eventTopic` is set, all events go to that single topic regardless of the document's run_id. This is safe in streaming mode because there is no Publisher performing per-run completion accounting. In batch mode with a Runner, do NOT set `kafka.eventTopic` — the per-run topic isolation is essential for correct completion detection.

| Mode | Recommended Setting |
|---|---|
| Batch (with Runner) | Omit `kafka.eventTopic` — let Lucille create per-run topics automatically |
| Streaming, no event tracking needed | `kafka.events: false` |
| Streaming, external event consumer | `kafka.eventTopic: "my_fixed_topic"` |

See [Streaming Mode Configuration]({{< relref "docs/operations/deployment" >}}) for the full streaming setup.

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

### Important: Source Topic Partition Count and Worker Threads

Lucille does **not** automatically create the source or dest topics. It only explicitly creates the event topic (with 1 partition for ordering). The source and dest topics are either:
- Pre-created by an administrator with the desired partition count, or
- Auto-created by Kafka when the Publisher first writes to them (if `auto.create.topics.enable=true` on the broker)

**The gotcha:** If Kafka auto-creates the source topic, the partition count is determined by the broker's `num.partitions` setting (default: **1**). This means that even if you configure `worker.threads: 8`, only 1 thread will receive documents — the other 7 will join the consumer group but sit idle because there's only 1 partition to assign.

Lucille does not validate at startup that the source topic has enough partitions for the configured number of worker threads. Excess consumers are simply idle — they don't error out.

**How to avoid this:**
- **Pre-create the source topic** with the desired partition count before running Lucille:
  ```bash
  kafka-topics.sh --create --topic my-pipeline_source \
    --partitions 8 --replication-factor 1 \
    --bootstrap-server kafka:9092
  ```
- **Or** configure the Kafka broker's `num.partitions` to a higher default (applies to all auto-created topics).
- **Rule of thumb:** Set the source topic partition count to at least the maximum total number of Worker threads you expect to run across all processes.

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

### Fail-Fast Policy

Lucille treats Kafka communication failures as non-recoverable. If the Kafka producer reports that a send has permanently failed — after exhausting its own internal retries (bounded by `delivery.timeout.ms`, default 2 minutes) — the run aborts. Lucille does not implement application-level retry logic on top of the producer's built-in retry mechanism. The reasoning:

- The Kafka producer already retries transient failures (network blips, leader elections) internally and transparently. A failure that surfaces to Lucille means retries were exhausted or the error is non-retryable.
- A run that has lost documents to a broken messaging path cannot produce a correct result. Continuing would silently drop data.
- Simplicity: the framework does not attempt partial recovery or compensating transactions. A failed run is restarted from the beginning.

This policy applies uniformly to all Kafka send paths — publishing documents, forwarding processed documents to the indexer, sending events, and sending to the dead-letter queue.

### Asynchronous vs. Synchronous Send Paths

Not all send paths use the same strategy. The choice depends on volume and whether offset-commit ordering is required.

| Path | Strategy | Rationale |
|------|----------|-----------|
| `KafkaPublisherMessenger.sendForProcessing` | **Asynchronous** — fire-and-forget with a callback; flushed at end of run | High volume. The publisher can emit thousands of documents per second. Blocking on each broker round trip would cap throughput at ~1 doc per RTT. |
| `KafkaWorkerMessenger.sendForIndexing` | **Synchronous** — `send().get()` | The source offset must not be committed until the processed document is durably on the dest topic. A synchronous ack guarantees this ordering. |
| `KafkaWorkerMessenger.sendFailed` | **Synchronous** — `send().get()` | Rare path (retry count exceeded). One record per exhausted-retry document; async provides no meaningful benefit. |
| Worker/Indexer `sendEvent` (single) | **Synchronous** — `send().get()` | Used for failure events and per-document cases. Low volume; per-document error attribution is straightforward when each send either succeeds or throws immediately. |
| Indexer `sendEvents` (batch) | **Asynchronous** — fire all sends with callbacks, flush once at end | After a successful index batch, completion events are sent for all documents in the batch. Sending them one-at-a-time with a broker round trip per event would add latency proportional to batch size. |

The synchronous paths call `.get()` on the returned `Future`, which blocks until the broker has acknowledged the record. No additional `flush()` is needed — the record is durably stored before execution continues.

### Error Surface Points (Publisher Path)

Because the publisher send path is asynchronous, a failure can occur after `sendForProcessing()` has returned. The error is surfaced in two places:

1. **At the start of the next `sendForProcessing()` call.** Before enqueuing a new record, the publisher checks whether a prior send failed (via a latched `AtomicReference<Exception>`). If so, it throws immediately. This prevents a connector from feeding documents to a destination that is no longer accepting them.

2. **In `PublisherMessenger.flush()`, called at the end of the publishing run.** This drains the producer's buffer (`kafkaProducer.flush()`) and then checks for any latched async failure. If the last document's send failed, this is where it surfaces.

In both cases, the thrown exception propagates up through `Publisher.publish()` or `Publisher.flush()` into the connector's `execute()` method, which terminates the run.

### The `Publisher.publish()` Contract

Exceptions thrown by `Publisher.publish()` represent framework-level failures — the messaging infrastructure is unavailable or has rejected a record after exhausting retries. These exceptions are **non-recoverable** and **must not be caught or swallowed** by connectors.

Connectors that need to handle per-document errors (e.g., a malformed source record that cannot be converted into a Document) should do so *before* calling `publish()`. Once `publish()` is called, any exception means the run should terminate. Catching and continuing past such an exception leaves the run in an inconsistent state: the Publisher's document-tracking accounting assumes every published document will eventually receive a terminal event, and a silently dropped document can never satisfy that assumption.

```java
// CORRECT: handle document-construction errors before publish
try {
    Document doc = buildDocumentFromRecord(record);
    publisher.publish(doc);
} catch (DocumentBuildException e) {
    log.warn("Skipping malformed record: {}", record.getId(), e);
    // This is fine — the document was never published, so no tracking state is affected
}

// WRONG: catching publish failures
try {
    publisher.publish(doc);
} catch (Exception e) {
    log.warn("Failed to publish, continuing...", e);
    // DO NOT DO THIS — the run is now in an inconsistent state
}
```

### Idempotent Producer

All Kafka producers created by Lucille have `enable.idempotence=true` set explicitly. This setting is forced and cannot be overridden, even via `kafka.producerPropertyFile`.

Why this matters: the asynchronous publish path sends multiple records without waiting for individual acknowledgments. The Kafka producer may have several batches in flight simultaneously (`max.in.flight.requests.per.connection` defaults to 5). Without idempotence, if a batch fails and is retried, it can land *after* a later batch that was already acknowledged — reordering records within a partition.

Since documents are keyed by their document ID, all operations on the same document land on the same partition. If a connector publishes a create and then a delete for the same document ID, reordering would apply the delete before the create, leaving the document in the wrong state. The idempotent producer prevents this by assigning sequence numbers that the broker uses to deduplicate retries and reject out-of-order batches.

Forcing idempotence also implicitly sets `acks=all` (the record must be replicated to all in-sync replicas before being acknowledged). This is the correct setting for Lucille: the Publisher stops tracking a document once the producer reports it accepted, so "accepted" must mean "durably replicated."

### Buffer Memory

The producer's `buffer.memory` controls the total bytes the accumulator can hold across all partitions — it bounds how many records can be waiting to be sent at any given time. This is unrelated to `max.request.size`, which bounds the size of a single record.

- `buffer.memory` defaults to 32 MB (Kafka's own default since 0.9.0.0), or to `kafka.maxRequestSize` if that exceeds 32 MB. This ensures a single oversized record can always fit in the accumulator while providing adequate space for async send batching.
- The Kafka producer property `max.request.size` is set from the Lucille config key `kafka.maxRequestSize` — it controls the maximum size of a single produce request sent to the broker.
