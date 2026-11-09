---
title: Messenger Abstraction
weight: 3
date: 2025-06-09
description: >
  The interfaces that make deployment-mode independence possible — LocalMessenger, TestMessenger, Kafka messengers, and the factory pattern.
---

## Overview

The Messenger layer is the architectural seam that makes Lucille deployment-mode independent. The same Connector, Worker, and Indexer code runs identically whether messages flow through in-memory queues, Kafka topics, or a hybrid of both. This is achieved through three interfaces and a factory pattern that selects the right implementation at runtime.

The messenger interfaces are internal to Lucille: they are not a supported extension point. The Runner chooses the messengers from the run type, and there is no setting for supplying your own, so a new transport means changing Lucille itself. The interfaces may change in any release, including a minor one.

## The Three Messenger Interfaces

Each Lucille component has its own messenger interface, exposing only the operations that component needs:

### WorkerMessenger

```java
public interface WorkerMessenger {
    Document pollDocToProcess() throws MessengerException;       // Receive work
    void commitPendingDocOffsets() throws MessengerException;    // Acknowledge work
    void sendForIndexing(Document document) throws MessengerException;  // Forward results
    void sendFailed(Document document) throws MessengerException;       // Dead letter queue
    void sendEvent(Document document, String message, Event.Type type) throws MessengerException;
    void sendEvent(Event event) throws MessengerException;       // Lifecycle events
    void close();
}
```

### IndexerMessenger

```java
public interface IndexerMessenger {
    Document pollDocToIndex() throws MessengerException;         // Receive processed docs
    void sendEvent(Event event) throws MessengerException;       // Report completion/failure
    void sendEvent(Document document, String message, Event.Type type) throws MessengerException;
    void sendEvents(List<Document> documents, String message, Event.Type type) throws MessengerException;
    void close();
    void batchComplete(List<Document> batch) throws MessengerException;  // Offset feedback

    // Optional capabilities (default implementations):
    default void setPreRevocationHook(Runnable hook) {}     // run before a Kafka partition is handed off
    default boolean commitsOnBatchCompletion() { return false; }  // required for indexer.maxConcurrentBatches > 1
}
```

`commitsOnBatchCompletion()` is true for messengers that commit (or hand off) their input only when `batchComplete()` is called, not when a document is polled. The Indexer requires it before it will accept `indexer.maxConcurrentBatches` greater than 1. `setPreRevocationHook()` lets the Kafka messenger ask the Indexer to flush and complete its in-progress batches before a consumer-group rebalance takes a partition away.

### PublisherMessenger

```java
public interface PublisherMessenger {
    void initialize(String runId, String pipelineName) throws MessengerException;
    String getRunId();
    void sendForProcessing(Document document) throws MessengerException;  // Publish to workers
    Event pollEvent() throws MessengerException;                          // Receive lifecycle events
    default void flush() throws MessengerException {}                     // Deliver buffered sends
    void close();
}
```

### MessengerException

Every messenger operation that can fail declares `MessengerException`, which wraps the transport's own exception as its cause. A messenger interrupted while waiting (on a full in-memory queue, for example) restores the thread's interrupt flag before throwing. For a send, `isDocumentSpecific()` tells the caller whether the transport rejected only this message, such as a Kafka record that is too large, so the caller can fail the document and carry on; any other failure means the messenger can no longer be relied on. `close()` declares no checked exception, since a caller shutting down cannot act on one. Errors that indicate a bug or a broken invariant, such as initializing a messenger twice, are unchecked.

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
- **`pollDocToProcess()`** — polls the source topic, blocking for up to `kafka.pollIntervalMs` (default 2000ms). Returns at most one record per poll (`MAX_POLL_RECORDS_CONFIG = 1`). Sets Kafka metadata on the returned `KafkaDocument`.
- **`commitPendingDocOffsets()`** — calls `sourceConsumer.commitSync()`. Offsets are committed synchronously to minimize reprocessing after crashes.
- **`sendForIndexing(doc)`** — produces to the dest topic using the document ID as the Kafka key. Calls `.get()` to wait for acknowledgment, so the record it came from is safe to commit once the document is sent.
- **`sendFailed(doc)`** — produces to the fail topic (dead letter queue).
- **`sendEvent(event)`** — serializes the event as JSON string and produces to the event topic.

## KafkaIndexerMessenger: Consume and Commit on Completion

Reads processed documents from the dest topic, and commits their offsets only after the batch containing them has been completed:

```java
public class KafkaIndexerMessenger implements IndexerMessenger {
    private final Consumer<String, KafkaDocument> destConsumer;
    private final KafkaProducer<String, String> kafkaEventProducer;
    private final OffsetCommitTracker offsetCommitTracker;
    private final Duration pollInterval;  // kafka.pollIntervalMs, default 2000ms
}
```

Key difference from the Worker: the Worker commits right after it has processed a document, but the Indexer does not commit when it polls. Polling only records the offset with the `OffsetCommitTracker`:

```java
public Document pollDocToIndex() {
    ConsumerRecords<String, KafkaDocument> consumerRecords = destConsumer.poll(pollInterval);
    if (consumerRecords.count() > 0) {
        // record the polled offset; nothing is committed yet
        offsetCommitTracker.recordPolled(partition, record.offset());
        // return the document
    }
    return null;
}

public void batchComplete(List<Document> batch) {
    offsetCommitTracker.batchCompleted(batch);  // advance the commit frontier and commit
}
```

`batchComplete()` is called by the Indexer once a batch has been handled: indexed, or reported failed with FAIL events. The tracker commits a per-partition *frontier*: the offset after the longest unbroken run of polled records whose batches have completed. Batches may finish out of order, and an earlier record may still be waiting in another batch, so the frontier never passes a record that has not been handled.

This is at-least-once delivery. If the Indexer crashes after a document was indexed but before its offset was committed, the document is redelivered and indexed again, which is safe because indexing is idempotent (upsert semantics), at the cost of a duplicate FINISH event. Nothing is lost. The document consumer must therefore be configured with `enable.auto.commit=false`, which Lucille checks when it creates the consumer.

On a consumer-group rebalance the tracker's rebalance listener first runs the Indexer's pre-revocation hook, which flushes the open batch and completes all in-flight batches, then commits their offsets while the partition is still owned. A clean rebalance therefore does not redeliver them. See [Delivery semantics]({{< relref "docs/operations/indexing-throughput#delivery-semantics-kafka-distributed-mode" >}}) for the details.

## HybridWorkerMessenger: Kafka In, Memory Out

The hybrid mode reads from Kafka but writes to an in-memory queue shared with a co-located Indexer. The worker's messenger commits the source offsets itself, from the Indexer's reports:

```java
public class HybridWorkerMessenger implements WorkerMessenger {
    private final Consumer<String, KafkaDocument> sourceConsumer;
    private final BlockingQueue<Document> pipelineDest;       // shared with the Indexer
    private final OffsetCommitTracker offsetCommitTracker;    // the same frontier as KafkaIndexerMessenger
    private final Queue<BatchResult> batchResults;            // the Indexer's reports, applied on the worker thread
}
```

Key behaviors:
- **`pollDocToProcess()`** — reads one record from Kafka and records its offset as polled. It first marks the record polled before it as processed.
- **`sendForIndexing(doc)`** — puts the document on the shared `pipelineDest` queue and remembers which polled record it came from. A child a stage created has no Kafka metadata of its own, so this is how its completion counts toward its parent's offset.
- **`commitPendingDocOffsets()`** — marks the record being processed as processed, applies the Indexer's reports, and commits the frontier.

A polled record is finished once the Worker has processed it and every document it sent for indexing (the record itself, its children, or both) has been reported complete. A record that sends nothing, because the pipeline dropped or failed it or it went to the dead letter queue, is finished as soon as the Worker is done with it. As in distributed mode, the tracker commits the offset after the longest unbroken run of finished records in each partition. So a dropped parent's offset is committed only once its children are indexed, and a record that never reaches the Indexer does not hold the frontier back.

**Why only hybrid mode needs this count.** In distributed mode, the destination topic sits between the Worker and the Indexer, and each commits its own offsets:

- **The Worker** commits a source record once every document made from it is on the destination topic. If the document was dropped or failed, nothing is sent, so the Worker commits right after sending the DROP or FAIL event.
- **The Indexer** reads one document from each destination record. Every offset it commits belongs to one document it indexed.

In hybrid mode there is no destination topic. Documents pass from the Worker to the Indexer in memory, so the source record's offset is the only one. It can't be committed until every document made from that record has been indexed, and that is what the count tracks.

If the Indexer reports a batch as not sent, because it could not confirm that the worker still owns the input, the records its documents came from stay unfinished, and after the next poll the worker rewinds the partition to the frontier so they are processed again. When partitions are revoked or lost, the documents from them still waiting in the queue, children included, are removed, so the partitions' next owner processes them.

This ensures offsets are only committed after everything a record produced has been indexed, providing at-least-once delivery without the Indexer having direct Kafka access.

### Walkthrough: One Record, Three Cases

For each polled record, the messenger keeps a count of what the record is still waiting for. The count starts at 1 for the Worker's processing of the record, each document sent for indexing adds 1, and each one the Indexer reports complete subtracts 1. The record is finished when the count reaches 0. The three cases below follow the record at partition 0, offset 5, with the Indexer's report arriving before the commit. The count exists only in hybrid mode; see *Why only hybrid mode needs this count* above.

**A document with no children**, indexed itself:

| Call | What happens | Count |
|---|---|---|
| `pollDocToProcess()` | Polls the record, records offset 5 as polled, and starts its count at 1 for the Worker's processing. | 1 |
| `sendForIndexing(doc)` | Queues the document for the Indexer and notes that it came from the record at offset 5. | 2 |
| `batchCompleted([doc])` | On the Indexer's thread, queues the report for the Worker thread. Nothing is counted yet. | 2 |
| `commitPendingDocOffsets()` | Subtracts 1 for the finished processing and 1 for the reported document. At 0, offset 5 completes and the frontier is committed if it moved. | 0 |

**A document with one child**, both indexed:

| Call | What happens | Count |
|---|---|---|
| `pollDocToProcess()` | Polls the record, records offset 5 as polled, and starts its count at 1. | 1 |
| `sendForIndexing(child)` | Queues the child and notes that it came from the record at offset 5, though the child has no Kafka metadata of its own. | 2 |
| `sendForIndexing(parent)` | Queues the parent and notes that it came from the record at offset 5. | 3 |
| `batchCompleted([child, parent])` | Queues the report for the Worker thread. | 3 |
| `commitPendingDocOffsets()` | Subtracts 1 for the finished processing and 1 for each reported document. At 0, offset 5 completes and the frontier is committed if it moved. | 0 |

**A dropped document**:

| Call | What happens | Count |
|---|---|---|
| `pollDocToProcess()` | Polls the record, records offset 5 as polled, and starts its count at 1. | 1 |
| (the pipeline drops it) | The Worker sends a DROP event, which the messenger does not count, and never calls `sendForIndexing`. | 1 |
| `commitPendingDocOffsets()` | Subtracts 1 for the finished processing. At 0, offset 5 completes at once, so it doesn't hold back the frontier. | 0 |

Two variations give the same result. If the Indexer reports after the commit, the count stops at 1 and the record is finished by a later poll or commit, once the report has been applied. If the child and the parent are in separate batches and the parent's is reported first, the record waits for the child. The count can't reach 0 while the Worker is still sending a record's documents, because the 1 for its processing is subtracted only after the Worker has sent them all, and reports are applied on the Worker thread, after the sends they report were counted.

## HybridIndexerMessenger: Memory In, Reports Out

The counterpart to `HybridWorkerMessenger`:

```java
public class HybridIndexerMessenger implements IndexerMessenger {
    private final BlockingQueue<Document> pipelineDest;        // shared with the Worker
    private final HybridWorkerMessenger workerMessenger;       // receives the batch reports
    private final KafkaProducer<String, String> kafkaEventProducer;
    private final Set<String> idSet;  // Optional: tracks unique indexed doc IDs
}
```

Key behaviors:
- **`pollDocToIndex()`** — takes from the shared `pipelineDest` queue (50ms timeout)
- **`sendEvent(event)`** — produces to the Kafka event topic (same as other Kafka messengers). Also adds the doc ID to `idSet` if configured.
- **`batchComplete(batch)`** — passes the batch to the worker messenger, which applies it on the worker thread before its next poll or commit.
- **`batchNotSent(batch)`** — likewise, for a batch the Indexer skipped because the ownership check failed.

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
