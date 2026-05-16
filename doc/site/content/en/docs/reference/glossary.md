---
title: "Concepts and Glossary"
weight: 5
date: 2025-06-09
description: >
  Definitions of key terms and concepts used throughout Lucille's documentation and codebase.
aliases:
  - /docs/glossary/
---

## Core Components

**Connector**
A component that reads data from a source system and publishes Documents into the pipeline. Each Connector has a lifecycle: `preExecute()` → `execute(publisher)` → `postExecute()` → `close()`. Connectors are configured in the `connectors` list in the config file and executed sequentially by the Runner.

**Worker**
A thread that polls Documents from the processing queue, runs them through a Pipeline of Stages, and places the results on the indexing queue. Multiple Worker threads can run concurrently, each with its own Pipeline instance. In distributed mode, Workers run as separate JVM processes.

**Indexer**
A component that polls processed Documents from the indexing queue, accumulates them in batches, and sends them to the search backend via its bulk API. The Indexer handles batching, retry, field filtering, and event reporting.

**Publisher**
The internal accounting component that tracks every Document from publication to terminal state. It determines when a run is complete by reconciling lifecycle events. The Publisher also provides backpressure (blocking the Connector when too many documents are in flight) and supports collapsing mode for CDC scenarios.

**Pipeline**
An ordered sequence of Stages. When a Document enters a Pipeline, it flows through each Stage in order. The Pipeline uses lazy iterator chaining so that documents are processed one at a time with bounded memory usage.

**Stage**
A single processing operation applied to a Document. Stages modify Documents in place and may optionally generate child Documents. Each Worker thread has its own instance of every Stage, so instance fields are thread-safe without synchronization.

**Runner**
The top-level orchestrator that coordinates a complete Lucille run: validates configuration, starts Workers and Indexer, launches Connectors sequentially, waits for completion, and reports results.

**Document**
The basic unit of data flowing through Lucille. A Document is an ordered set of named fields (single-valued or multi-valued) backed by a Jackson ObjectNode. Every Document has an immutable `id` field.

---

## Execution Concepts

**Run**
A single execution of one or more Connectors in sequence. Each Connector's work must complete before the next begins. A run has a unique `run_id` (UUID) that is stamped on every Document published during that run.

**Run ID**
A UUID that uniquely identifies a run. Stamped on every Document by the Publisher at publication time. Used for log correlation, event routing, and Kafka topic naming.

**Batch Ingest**
An execution model where a finite set of data is retrieved from a source system, processed, and indexed. The run has a clear beginning and end. The Publisher provides exact completion detection.

**Streaming Mode**
An execution model where an unbounded supply of documents arrives continuously. There is no run boundary, no completion accounting, and no Connector or Publisher. Workers consume directly from a Kafka topic populated by an external system.

**Connector Timeout**
The maximum time (default: 24 hours) the Runner will wait for a Connector's work to complete before declaring the run failed. Configured via `runner.connectorTimeout`.

---

## Document Concepts

**Field**
A named value on a Document. Fields can be single-valued (`setField`) or multi-valued (`addToField`). Supported types: String, Boolean, Integer, Double, Float, Long, Instant, byte[], JsonNode, Timestamp, Date.

**Multi-Valued Field**
A field containing a list of values rather than a single value. Created by calling `addToField()` on an existing field, or by using `setOrAdd()` multiple times.

**UpdateMode**
An enum controlling how a field write behaves: `OVERWRITE` (replace existing values), `APPEND` (add to existing values), or `SKIP` (write only if the field doesn't already exist).

**Reserved Fields**
Internal control fields that cannot be written to by Stages: `id`, `run_id`, `___children`, `___dropped`, `___skipped`. These are stripped by the Indexer before sending to the search backend.

**Dropped Document**
A Document marked with `___dropped = true`. It is ignored by all downstream Stages and is NOT sent to the Indexer. The Worker sends a DROP event to the Publisher. Use this to filter out documents entirely.

**Skipped Document**
A Document marked with `___skipped = true`. It is ignored by all downstream Stages but IS still sent to the Indexer. Used for deletion markers — the document bypasses enrichment but reaches the Indexer so it can issue a delete against the search backend.

**Child Document (Attached)**
A Document stored inside a parent via `doc.addChild(childDoc)`. Attached children travel with the parent as nested data. They are not independently tracked by the Publisher and are not indexed as separate records.

**Child Document (Emitted)**
A Document returned from `processDocument()` as part of an Iterator. Emitted children become independent, first-class documents that flow through downstream Stages, are tracked by the Publisher (via CREATE events), and are indexed as their own records.

**Document ID**
The immutable identifier assigned to a Document at creation time. Used as the Kafka message key (for ordering), the Publisher's tracking key (for accounting), and the primary key in the search index (for upsert semantics). Must be deterministic and stable across re-runs.

**ID Override (idOverrideField)**
A configuration option that tells the Indexer to use a different field's value as the document's ID in the search index, without changing the internal tracking ID.

**Doc ID Prefix (docIdPrefix)**
A string prepended to all document IDs created by a Connector, used to namespace IDs and prevent collisions when multiple Connectors feed the same index.

---

## Messaging Concepts

**Messenger**
An interface that abstracts inter-component communication. Each component type has its own Messenger interface (`PublisherMessenger`, `WorkerMessenger`, `IndexerMessenger`). Implementations include `LocalMessenger` (in-memory queues), `TestMessenger` (recording wrapper), and Kafka-based messengers.

**Processing Queue (Source Queue)**
The queue between the Connector/Publisher and the Workers. Documents waiting to be enriched sit here. In local mode: a `LinkedBlockingQueue`. In distributed mode: a Kafka topic named `{pipeline}_source`.

**Indexing Queue (Destination Queue)**
The queue between the Workers and the Indexer. Processed documents waiting to be sent to the search backend sit here. In local mode: a `LinkedBlockingQueue`. In distributed mode: a Kafka topic named `{pipeline}_dest`.

**Event Queue**
The queue carrying lifecycle events (CREATE, FINISH, FAIL, DROP) from Workers and Indexers back to the Publisher. In local mode: a `LinkedBlockingQueue`. In distributed mode: a Kafka topic named `{pipeline}_event_{runId}`.

**Event**
A lifecycle notification about a Document. Types: `CREATE` (child document generated), `FINISH` (successfully indexed), `FAIL` (processing or indexing error), `DROP` (explicitly dropped by a Stage).

---

## Deployment Concepts

**Local Mode**
All components run as threads in a single JVM. Communication uses in-memory queues. The default mode for development and small production jobs.

**Test Mode**
Same as Local mode, but the search backend is bypassed and all message traffic is recorded by a `TestMessenger` for assertion by test code.

**Kafka-Local Mode**
All components run as threads in a single JVM, but communicate via Kafka topics. Useful for testing Kafka integration without deploying separate processes.

**Kafka-Distributed Mode**
Workers and Indexers run as separate JVM processes. The Runner only launches Connectors and waits for completion via Kafka events. The production scale-out model.

**WorkerIndexer**
A deployment pattern that pairs a Worker and an Indexer in a single JVM process. The Worker reads from Kafka but writes to an in-memory queue consumed by the co-located Indexer. Provides horizontal scaling without the operational complexity of separate Worker and Indexer fleets.

**WorkerIndexerPool**
Manages a pool of WorkerIndexer pairs within a single JVM. Configured via `worker.threads`.

**WorkerPool**
Manages a pool of Worker threads within a single JVM. Includes a watcher thread for periodic stats logging and stuck-worker detection.

---

## Reliability Concepts

**Backpressure**
A mechanism that prevents a fast Connector from overwhelming downstream components. In local mode: bounded queue capacity (`publisher.queueCapacity`). In distributed mode: `publisher.maxPendingDocs` blocks `publish()` when too many documents are in flight.

**Poison Pill**
A document that repeatedly causes a Worker process to crash (e.g., due to a bug in third-party code triggered by malformed input). Detected by a `RetryCounter` backed by ZooKeeper or Redis. After exceeding `worker.maxRetries`, the document is routed to the dead letter queue.

**Dead Letter Queue (Fail Topic)**
A Kafka topic (`{pipeline}_fail`) where poison-pill documents are sent after exceeding their retry limit. Documents here can be inspected, fixed, and replayed.

**At-Least-Once Delivery**
Lucille's delivery guarantee in distributed mode. A document may be processed more than once if a crash occurs between processing and offset commit, but it will never be silently lost.

**Offset Commit**
The act of telling Kafka that a message has been successfully processed and should not be redelivered. In Lucille, offsets are committed synchronously after processing (Worker) or after indexing (WorkerIndexer hybrid mode).

**Consumer Group Rebalance**
A Kafka mechanism that redistributes partitions among consumers when a consumer joins or leaves the group. In Lucille, this happens when a Worker or Indexer process starts or stops. Rebalances can cause brief reprocessing of in-flight documents.

---

## Configuration Concepts

**HOCON**
Human-Optimized Config Object Notation — the configuration format used by Lucille. A superset of JSON that supports comments, file includes, variable substitution, and relaxed syntax. Parsed by the Typesafe Config library.

**SPEC**
A static declaration on every Stage, Connector, and Indexer that defines its legal configuration properties (required/optional, types). Validated before the run starts to catch config errors at startup.

**ConfigUtils.getOrDefault**
A utility method for reading optional config properties with a default value: `ConfigUtils.getOrDefault(config, "key", defaultValue)`.

**Environment Variable Substitution**
HOCON's `${?ENV_VAR}` syntax for optionally overriding a config value from an environment variable. The `?` makes it optional — if the env var is not set, the previous value stands.

---

## Indexing Concepts

**Batch**
A group of documents accumulated by the Indexer before sending to the search backend in a single bulk API call. Flushes when `indexer.batchSize` is reached or `indexer.batchTimeout` milliseconds have elapsed.

**MultiBatch**
A variant of Batch that maintains separate batches per destination index. Used when `indexer.indexOverrideField` is configured and documents in the same run go to different indices.

**Deletion Marker**
A document field/value combination (`indexer.deletionMarkerField` + `indexer.deletionMarkerFieldValue`) that signals the Indexer to issue a delete operation instead of an index operation.

**Delete-by-Query**
A deletion mode where the Indexer deletes all documents in the index matching a field/value pair, rather than deleting a single document by ID. Configured via `indexer.deleteByFieldField` and `indexer.deleteByFieldValue`.

**Field Filtering (Whitelist/Blacklist)**
Configuration that controls which document fields are sent to the search backend. `indexer.whitelist` includes only listed fields; `indexer.blacklist` excludes listed fields. Reserved fields are always stripped.

**Bypass Mode**
When `bypass = true` (test mode), the Indexer skips all actual backend communication. Documents are still batched and events are still sent, but `sendToIndex()` is not called.

---

## Metrics Concepts

**Codahale Metrics (Dropwizard Metrics)**
The metrics library used by Lucille. Provides Timers, Meters, Counters, and Histograms registered in a shared `MetricRegistry`.

**One Minute Rate**
An exponentially weighted moving average (EWMA) of throughput with a 1-minute half-life. Not a simple count of events in the last 60 seconds. Reported by the WorkerPool watcher and Indexer.

**Mean Pipeline Latency**
The average time (ms/doc) to process a single document through all pipeline stages. Measured per-document, per-thread. With N threads, theoretical throughput = N × (1000 / latency).

**Mean Backend Latency**
The average time (ms/doc) for the search engine to accept a batch, normalized by batch size (total batch time / number of documents in batch).

**WorkerWatcherExecutorService**
A daemon thread started by the WorkerPool that logs periodic pipeline statistics, detects stuck workers, and emits heartbeats for Kubernetes liveness probes.

**Heartbeat**
A periodic log message written to a dedicated logger (`com.kmwllc.lucille.core.Heartbeat`) that Kubernetes liveness probes can monitor. Enabled via `worker.enableHeartbeat`.

---

## Testing Concepts

**RunType.TEST**
A run mode that executes the full pipeline end-to-end with in-memory messaging and a bypassed indexer. All message traffic is captured for assertion.

**TestMessenger**
A wrapper around `LocalMessenger` that records every document published, every document sent for indexing, and every lifecycle event. Accessible after a test run for assertions.

**StageFactory**
A test utility that instantiates and starts a Stage from a config file, handling the boilerplate of reflective construction and `start()` invocation.

**Validation Mode**
Running Lucille with the `-validate` flag checks all configuration without executing any connectors. Reports all errors at once.
