---
title: Messenger Abstraction
weight: 3
date: 2025-06-09
description: >
  The interfaces that make deployment-mode independence possible — LocalMessenger, TestMessenger, Kafka messengers, and the factory pattern.
---

## Overview

The Messenger layer is the architectural seam that makes Lucille deployment-mode independent. The same Connector, Worker, and Indexer code runs identically whether messages flow through in-memory queues, Kafka topics, or a hybrid of both. This is achieved through three interfaces and a factory pattern that selects the right implementation at runtime.

## The Three Messenger Interfaces

Each Lucille component has its own messenger interface, exposing only the operations that component needs:

### WorkerMessenger

```java
public interface WorkerMessenger {
    Document pollDocToProcess() throws Exception;       // Receive work
    void commitPendingDocOffsets() throws Exception;    // Acknowledge work
    void sendForIndexing(Document document) throws Exception;  // Forward results
    void sendFailed(Document document) throws Exception;       // Dead letter queue
    void sendEvent(Document document, String message, Event.Type type) throws Exception;
    void sendEvent(Event event) throws Exception;       // Lifecycle events
    void close() throws Exception;
}
```

### IndexerMessenger

```java
public interface IndexerMessenger {
    Document pollDocToIndex() throws Exception;         // Receive processed docs
    void sendEvent(Event event) throws Exception;       // Report completion/failure
    void sendEvent(Document document, String message, Event.Type type) throws Exception;
    void close() throws Exception;
    void batchComplete(List<Document> batch) throws Exception;  // Offset feedback
}
```

### PublisherMessenger

```java
public interface PublisherMessenger {
    void initialize(String runId, String pipelineName) throws Exception;
    String getRunId();
    void sendForProcessing(Document document) throws Exception;  // Publish to workers
    Event pollEvent() throws Exception;                          // Receive lifecycle events
    void close();
}
```

## LocalMessenger: One Class, Three Interfaces

`LocalMessenger` implements all three interfaces using shared `LinkedBlockingQueue` instances:

```java
public class LocalMessenger implements IndexerMessenger, PublisherMessenger, WorkerMessenger {
    private final BlockingQueue<Event> pipelineEvents = new LinkedBlockingQueue<>();
    private final BlockingQueue<Document> pipelineSource;  // Publisher → Worker
    private final BlockingQueue<Document> pipelineDest;    // Worker → Indexer
}
```

The data flow:
- `sendForProcessing(doc)` → puts on `pipelineSource`
- `pollDocToProcess()` → takes from `pipelineSource`
- `sendForIndexing(doc)` → puts on `pipelineDest`
- `pollDocToIndex()` → takes from `pipelineDest`
- `sendEvent(event)` → puts on `pipelineEvents`
- `pollEvent()` → takes from `pipelineEvents`

All polls use a 50ms timeout (`POLL_TIMEOUT_MS`) to allow polling loops to check termination conditions.

Queue capacity is configurable via `publisher.queueCapacity` (default: 10,000). The `put()` calls will block if the queue is full, providing natural backpressure.

Key simplifications in local mode:
- `commitPendingDocOffsets()` is a no-op (no offsets to commit)
- `sendFailed()` is a no-op (no dead letter queue)
- `batchComplete()` is a no-op (no offset feedback needed)
- `close()` is a no-op (queues are garbage collected)

## TestMessenger: Recording History

`TestMessenger` wraps a `LocalMessenger` and intercepts writes to record message history:

```java
public class TestMessenger implements IndexerMessenger, PublisherMessenger, WorkerMessenger {
    private final LocalMessenger messenger;
    private List<Event> savedEventMessages = Collections.synchronizedList(new ArrayList<>());
    private List<Document> savedSourceMessages = Collections.synchronizedList(new ArrayList<>());
    private List<Document> savedDestMessages = Collections.synchronizedList(new ArrayList<>());
}
```

The interception points:
- `sendForProcessing(doc)` → saves to `savedSourceMessages`, then delegates
- `sendForIndexing(doc)` → saves to `savedDestMessages`, then delegates
- `sendEvent(event)` → saves to `savedEventMessages`, then delegates

After a test run, you can inspect:
- `getDocsSentForProcessing()` — documents the connector published
- `getDocsSentForIndexing()` — documents that completed pipeline processing
- `getSentEvents()` — all lifecycle events (CREATE, FINISH, FAIL, DROP)

The lists are `synchronizedList` because Worker and Indexer threads write concurrently.

## KafkaWorkerMessenger: Full Kafka Mode

Reads documents from a Kafka source topic, writes processed documents to a dest topic, and sends events to an event topic:

```java
public class KafkaWorkerMessenger implements WorkerMessenger {
    private final Consumer<String, KafkaDocument> sourceConsumer;
    private final KafkaProducer<String, Document> kafkaDocumentProducer;
    private final KafkaProducer<String, String> kafkaEventProducer;
}
```

Key behaviors:
- **`pollDocToProcess()`** — polls the source topic with `KafkaUtils.POLL_INTERVAL` (2 seconds). Returns at most one record per poll (`MAX_POLL_RECORDS_CONFIG = 1`). Sets Kafka metadata on the returned `KafkaDocument`.
- **`commitPendingDocOffsets()`** — calls `sourceConsumer.commitSync()`. Offsets are committed synchronously to minimize reprocessing after crashes.
- **`sendForIndexing(doc)`** — produces to the dest topic using the document ID as the Kafka key. Calls `.get()` to wait for acknowledgment, then `flush()`.
- **`sendFailed(doc)`** — produces to the fail topic (dead letter queue).
- **`sendEvent(event)`** — serializes the event as JSON string and produces to the event topic.

## KafkaIndexerMessenger: Consume and Commit

Reads processed documents from the dest topic:

```java
public class KafkaIndexerMessenger implements IndexerMessenger {
    private final Consumer<String, KafkaDocument> destConsumer;
    private final KafkaProducer<String, String> kafkaEventProducer;
}
```

Key difference from the Worker: offsets are committed **immediately after polling**, before the document is indexed:

```java
public Document pollDocToIndex() throws Exception {
    ConsumerRecords<String, KafkaDocument> consumerRecords = destConsumer.poll(KafkaUtils.POLL_INTERVAL);
    if (consumerRecords.count() > 0) {
        destConsumer.commitSync();  // Commit immediately
        // return the document
    }
    return null;
}
```

This means a document might be indexed twice if the indexer crashes after committing but before sending the FINISH event. This is acceptable because indexing is idempotent (upsert semantics).

## HybridWorkerMessenger: Kafka In, Memory Out

The hybrid mode reads from Kafka but writes to an in-memory queue shared with a co-located Indexer:

```java
public class HybridWorkerMessenger implements WorkerMessenger {
    private final Consumer<String, KafkaDocument> sourceConsumer;
    private final KafkaProducer<String, String> kafkaEventProducer;
    private final LinkedBlockingQueue<Document> pipelineDest;           // Shared with Indexer
    private final LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets;  // From Indexer
}
```

Key behaviors:
- **`pollDocToProcess()`** — reads from Kafka (same as `KafkaWorkerMessenger`)
- **`sendForIndexing(doc)`** — puts on the shared `pipelineDest` queue (in-memory, not Kafka)
- **`commitPendingDocOffsets()`** — drains the `offsets` queue and commits each batch of offsets to Kafka. These offsets come from the Indexer after it successfully indexes documents.

The offset feedback loop:
1. Worker polls document from Kafka (records topic/partition/offset)
2. Worker processes document, puts on `pipelineDest`
3. Indexer picks up document, indexes it, puts offset info on `offsets` queue
4. Worker's next `commitPendingDocOffsets()` call commits those offsets to Kafka

This ensures offsets are only committed after documents are actually indexed, providing at-least-once delivery guarantees without requiring the Indexer to have direct Kafka access.

## HybridIndexerMessenger: Memory In, Offsets Out

The counterpart to `HybridWorkerMessenger`:

```java
public class HybridIndexerMessenger implements IndexerMessenger {
    private final LinkedBlockingQueue<Document> pipelineDest;
    private final LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets;
    private final KafkaProducer<String, String> kafkaEventProducer;
    private final Set idSet;  // Optional: tracks unique indexed doc IDs
}
```

Key behaviors:
- **`pollDocToIndex()`** — takes from the shared `pipelineDest` queue (50ms timeout)
- **`sendEvent(event)`** — produces to the Kafka event topic (same as other Kafka messengers). Also adds the doc ID to `idSet` if configured.
- **`batchComplete(batch)`** — extracts Kafka metadata from each `KafkaDocument` in the batch, builds an offset map, and puts it on the `offsets` queue for the Worker to commit:

```java
public void batchComplete(List<Document> batch) throws InterruptedException {
    Map<TopicPartition, OffsetAndMetadata> batchOffsets = new HashMap<>();
    for (Document doc : batch) {
        if (!(doc instanceof KafkaDocument)) continue;
        KafkaDocument kDoc = (KafkaDocument) doc;
        TopicPartition tp = new TopicPartition(kDoc.getTopic(), kDoc.getPartition());
        OffsetAndMetadata offset = new OffsetAndMetadata(kDoc.getOffset() + 1);  // +1 per Kafka convention
        batchOffsets.put(tp, offset);
    }
    if (!batchOffsets.isEmpty()) {
        offsets.put(batchOffsets);
    }
}
```

## The MessengerFactory Pattern

Each messenger type has a factory interface:

```java
public interface WorkerMessengerFactory {
    WorkerMessenger create();

    static WorkerMessengerFactory getConstantFactory(WorkerMessenger messenger) {
        return () -> messenger;  // Always returns the same instance
    }

    static WorkerMessengerFactory getKafkaFactory(Config config, String pipelineName) {
        return () -> new KafkaWorkerMessenger(config, pipelineName);  // New instance each time
    }
}
```

The `getConstantFactory` is used for LOCAL/TEST modes where a single messenger instance is shared. The `getKafkaFactory` creates a new messenger (with its own Kafka consumer) for each Worker thread — necessary because Kafka consumers are not thread-safe.

## How the Runner Uses Factories

```java
if (RunType.TEST.equals(type)) {
    TestMessenger messenger = new TestMessenger();
    workerMessengerFactory = WorkerMessengerFactory.getConstantFactory(messenger);
    indexerMessengerFactory = IndexerMessengerFactory.getConstantFactory(messenger);
    publisherMessengerFactory = PublisherMessengerFactory.getConstantFactory(messenger);
} else if (RunType.LOCAL.equals(type)) {
    LocalMessenger messenger = new LocalMessenger(config);
    workerMessengerFactory = WorkerMessengerFactory.getConstantFactory(messenger);
    indexerMessengerFactory = IndexerMessengerFactory.getConstantFactory(messenger);
    publisherMessengerFactory = PublisherMessengerFactory.getConstantFactory(messenger);
} else {
    workerMessengerFactory = WorkerMessengerFactory.getKafkaFactory(config, pipelineName);
    indexerMessengerFactory = IndexerMessengerFactory.getKafkaFactory(config, pipelineName);
    publisherMessengerFactory = PublisherMessengerFactory.getKafkaFactory(config);
}
```

## Architectural Significance

The Messenger abstraction is what makes Lucille's deployment flexibility possible:

- **Development/Testing**: Use `LocalMessenger` or `TestMessenger` — no external dependencies, fast, inspectable
- **Single-node production**: Use `LocalMessenger` — all components in one JVM, no Kafka overhead
- **Distributed production**: Use Kafka messengers — Workers and Indexers can scale independently across machines
- **Hybrid**: Use Hybrid messengers — read from Kafka for distribution, but avoid Kafka overhead for the Worker→Indexer hop within the same JVM

The components (Worker, Indexer, Publisher) never know which messenger implementation they're using. They code against the interface, and the Runner wires in the appropriate implementation at startup.
