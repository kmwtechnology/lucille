---
title: "What the Framework Gives You"
weight: 5
date: 2025-06-09
description: >
  What you are really getting when you adopt Lucille — the division of labor between framework and implementor.
---

When you are developing components for Lucille, it is important to understand what the framework does and does not give you. That understanding lets you take full advantage of what is already built, and gives you realistic expectations about the responsibilities that will fall to you as the implementor.

This page begins with a general discussion of what you get when you adopt the framework overall. The following sections then focus on each component type — Stage, Connector, and Indexer. When you implement a component you will be extending the `Stage` or `Indexer` abstract classes, or implementing the `Connector` interface. Each section explains how the framework interacts with that component, what the base class or framework infrastructure does for you automatically, and what you are responsible for writing yourself.

---

## What You Are Really Getting When You Adopt Lucille

When you adopt Lucille, you are not just getting a library of connectors and stages. You are getting a runtime that solves the hard problems of production search ingestion — problems that are invisible during a POC but dominate the engineering effort as a system matures.

**You are getting a concurrency model you don't have to build.** Reading, enriching, and indexing run as independent components communicating through queues. You don't write threading code, you don't manage shared state between components, and you don't debug race conditions. The framework handles the concurrent execution; you write sequential logic inside each component.

**You are getting completion detection you don't have to invent.** In a distributed system where multiple workers and indexers process documents asynchronously — and where new child documents can be generated mid-pipeline — knowing when "all the work is done" is a genuinely hard problem. Lucille's Publisher solves it with an event-driven accounting model that tracks every document from publication to terminal state. You call `publisher.publish(doc)` and the framework tells you when everything is finished.

**You are getting fault tolerance you don't have to implement.** Proper Kafka offset management, consumer group rebalancing, at-least-once delivery guarantees, and poison pill detection are built into the framework's messaging layer. You don't write offset commit logic or crash recovery code. You write a stage that processes one document at a time, and the framework ensures that document is never silently lost.

**You are getting a deployment model you don't have to choose upfront.** The same pipeline code runs in a single JVM for development and in a fully distributed Kafka deployment for production. You don't architect for scale on day one and you don't rewrite when scale arrives. The framework's pluggable messenger abstraction means the transition from local to distributed is a command-line flag, not a code change.

**You are getting batching, retry, and error handling you don't have to get right.** Sending documents to a search engine in production involves batch accumulation with dual thresholds, exponential backoff on transient failures, per-document vs. batch-level failure discrimination, and correct event reporting for every outcome. The base Indexer class handles all of this. You implement one method — `sendToIndex(List<Document>)` — and the framework handles everything around it.

**You are getting a test infrastructure that makes correctness verifiable.** The test mode captures the complete history of every document through the system — what was published, what was processed, what was indexed, what failed. You write assertions against this history without mocking the framework itself. The system under test is the real system, running in memory, with the search backend bypassed.

**You are getting a configuration system that scales from a single file to a multi-team deployment.** HOCON with environment variable substitution, file includes, and pre-run validation means your pipeline definition works unchanged from a developer's laptop to a Kubernetes CronJob to a distributed Kafka deployment. Credentials come from environment variables. Shared settings come from included files. Typos are caught before the run starts.

**You are getting a Document API designed for search.** Single-valued and multi-valued fields, typed access without casts, update modes that match how search engines actually work, nested JSON support, and zero-cost serialization to the wire format that every search backend expects. Every stage you write is a few lines of domain logic rather than a page of field-access boilerplate.

**You are getting a library of enrichment stages you don't have to write.** Text extraction, OCR, NER, embeddings, chunking, database lookups, HTTP enrichment, scripting, and dozens of field manipulation operations — all configurable, all composable, all tested. For many pipelines, the custom code you write is zero: the pipeline is pure configuration.

**What you are not getting** is a visual workflow editor, a managed cloud service, or a connector catalog for hundreds of SaaS sources. Lucille is a framework for engineers who write code. Its value proposition is that the code you write is small, focused on your specific problem, and surrounded by a runtime that handles everything else correctly.

---

The remainder of this document details exactly how this division of labor works for each component type — what the framework handles, and what you as the implementor are responsible for.

---

## Stages

### What the framework handles (you don't have to)

**Conditional execution.** The base `Stage` class evaluates the `conditions` configuration block before calling `processDocument()`. If conditions don't match, the stage is skipped entirely. You never write `if (doc.has("myField"))` checks for basic field-presence gating — configure it in HOCON and the framework handles it.

**Dropped and skipped document handling.** If a document is marked as dropped or skipped, `shouldProcess()` returns false and `processDocument()` is never called. You don't need to check these flags.

**Per-stage metrics.** The framework automatically tracks:
- Processing time per document (a Codahale Timer)
- Error count (a Counter incremented when `processDocument` throws)
- Child document count (a Counter incremented for each child emitted)

These are registered with a shared `MetricRegistry` and reported at the end of the run. You get per-stage performance visibility for free.

**Per-document logging.** The DocLogger automatically logs stage entry and exit for every document processed, with the stage name and document ID. You don't need to add logging for "stage X processed document Y."

**Child document lifecycle management.** When `processDocument()` returns an iterator of children, the framework:
- Copies the parent's run ID to each child
- Counts children in the metrics
- Ensures children flow through downstream stages (but not upstream ones)
- Ensures children are emitted before the parent in the result iterator (so the Publisher learns about children before the parent completes)

**Thread safety via per-thread instantiation.** Each Worker thread gets its own Pipeline with its own Stage instances. You can use instance fields freely without synchronization. The framework guarantees this isolation.

**Configuration validation.** The SPEC you declare is validated before the run starts. If a user provides an unrecognized parameter or omits a required one, the error is caught at startup — not when your stage tries to read it.

**Automatic naming.** If no `name` is configured, the framework assigns one based on position (`stage_1`, `stage_2`, etc.).

**Reflective instantiation.** The framework instantiates your stage from the `class` property in config. You don't register it anywhere — just put it on the classpath.

### What you must handle (with guidance from existing implementations)

**Resource initialization and cleanup.** If your stage needs external resources (database connections, HTTP clients, loaded models), initialize them in `start()` and clean them up in `stop()`. The framework calls these at the right time but doesn't know what resources you need. See `FetchUri` (HTTP client), `DatabaseLookup` (JDBC connection), `JlamaEmbed` (model loading).

**Error semantics.** Decide whether an error should fail the document (throw `StageException`) or be handled gracefully (log and continue). The framework catches `StageException` and routes the document to a failure state. If you catch exceptions internally and continue, the document proceeds through the pipeline. See `FetchUri` for graceful degradation (logs the error, leaves the field empty) vs. stages that throw on any failure.

**Child document creation.** If your stage generates children, you must create them with unique IDs, populate their fields, and return them as an iterator. The framework handles everything after that. See `ChunkText` (attaches children to parent) and `EmitNestedChildren` (converts attached children to emitted children).

**Idempotency.** In distributed mode, a document may be processed more than once (crash + redelivery). Your stage should produce the same result if called twice on the same input. Most stages are naturally idempotent (setting a field to a computed value is idempotent). Stages with side effects (writing to an external system, incrementing a counter) need explicit consideration.

**Multi-valued field handling.** Decide whether your stage should process the first value of a field (`getString`) or all values (`getStringList`). The Document API supports both patterns, but you must choose the right one for your use case.

---

## Connectors

### What the framework handles (you don't have to)

**Lifecycle orchestration.** The Runner calls `preExecute()`, `execute()`, `postExecute()`, and `close()` in the correct order with the correct error-handling semantics:
- `execute()` is not called if `preExecute()` throws
- `postExecute()` is not called if `execute()` throws
- `close()` is always called regardless of success or failure
- The run is aborted if any lifecycle method throws

You implement the methods; the framework calls them at the right time.

**Document publication and tracking.** The `Publisher` passed to `execute()` handles:
- Stamping the run ID on each document
- Tracking document IDs for completion accounting
- Backpressure (blocking `publish()` when too many documents are in flight)
- Thread safety (multiple threads can call `publish()` concurrently)
- Collapsing mode (merging consecutive same-ID documents if configured)

You just call `publisher.publish(doc)` and the framework handles everything else.

**Sequential connector composition.** If multiple connectors are configured, the Runner ensures each one completes fully (all documents processed and indexed) before the next starts. You don't coordinate with other connectors.

**Configuration validation.** The SPEC declared in your connector is validated before the run starts. The `AbstractConnector` base class handles parsing common config (name, pipeline, docIdPrefix, collapse) so you don't repeat it.

**Doc ID prefixing.** If `docIdPrefix` is configured, `AbstractConnector.createDocId(id)` prepends it. You call this helper when creating document IDs.

**Reflective instantiation.** Like stages, connectors are instantiated from the `class` property in config.

### What you must handle (with guidance from existing implementations)

**Source system connection management.** Open connections in `preExecute()` or at the start of `execute()`. Close them in `close()`. The framework doesn't know how to connect to your source. See `FileConnector` (initializes StorageClients in `execute()`), `DatabaseConnector` (opens JDBC in `preExecute()`).

**Document creation with meaningful IDs.** Each document needs a unique ID. The ID should be stable across re-runs (so re-ingestion updates rather than duplicates). See `FileConnector` (uses file path as ID), `DatabaseConnector` (uses a configured ID column).

**Incremental state tracking.** If your connector needs to track what it has already published (to avoid re-processing unchanged data), you must implement that state management. See `FileConnector`'s `FileConnectorStateManager` (JDBC-backed state tracking of published files and their modification times).

**Error handling during iteration.** If reading one record from the source fails, decide whether to skip it and continue or abort the connector. The framework aborts the run if `execute()` throws, but individual record failures within `execute()` are your responsibility to handle. See `FileConnector` (logs and continues on individual file errors) vs. `DatabaseConnector` (wraps the entire query in a try-catch).

**Pagination and memory management.** If the source has millions of records, you can't load them all into memory. Use streaming/cursor-based access and publish documents as you go. The Publisher's backpressure (`maxPendingDocs`) will block you if you're publishing faster than the system can process, but you must still avoid loading the entire dataset into memory before publishing. See `DatabaseConnector` (uses JDBC fetch size for streaming).

**Tombstone/deletion document generation.** If your connector needs to signal deletions (records that existed in a previous run but are now gone), you must create documents marked for deletion. See `FileConnector`'s `sendExpiredFileTombstones()` (creates skipped documents with an `expired` flag for files no longer in storage).

---

## Indexers

### What the framework handles (you don't have to)

**Batching with size and timeout.** The base `Indexer` class accumulates documents in a `Batch` and flushes when either the configured `batchSize` is reached or `batchTimeout` milliseconds have elapsed. Your `sendToIndex()` method receives a pre-batched list of documents. You never manage batch accumulation or timeout logic.

**MultiBatch for index routing.** When `indexOverrideField` is configured, the framework uses a `MultiBatch` that maintains separate batches per destination index, flushing them independently. Your implementation receives documents already grouped by destination.

**Retry with exponential backoff.** When `indexer.maxRetries` is configured, the framework wraps your `sendToIndex()` call in a Resilience4j retry. If you throw an `IndexerRetryableException` with a status code in the configured retryable list, the framework retries automatically with exponential backoff. You just throw the right exception type; the framework handles retry logic.

**Event reporting for accounting.** After each batch, the framework sends FINISH events for successful documents and FAIL events for failed ones. You return a set of failed document/reason pairs from `sendToIndex()`; the framework handles all event communication with the Publisher.

**Field filtering (whitelist/blacklist).** The `getIndexerDoc()` method applies the configured field filter before you see the document. Reserved fields (`___dropped`, `___skipped`, `___children`) are stripped. You call `getIndexerDoc(doc)` and get a clean map ready for the search backend.

**ID override.** `getDocIdOverride(doc)` returns the override ID if configured, or null. You check this and use it as the document ID in the search backend.

**Index override.** `getIndexOverride(doc)` returns the destination index/collection override if configured, or null.

**Connection validation.** The framework calls `validateConnection()` before processing any documents. If it returns false, the run is aborted immediately — no documents are wasted on a broken connection.

**Offset commitment (in WorkerIndexer mode).** The `batchComplete()` call in the `finally` block of `sendToIndexWithAccounting` always fires, allowing the messenger to commit Kafka offsets. You don't manage offset semantics.

**Graceful shutdown.** The `terminate()` method sets a flag that causes the polling loop to exit after the current batch. The framework handles the shutdown sequence.

**Metrics.** The framework tracks indexing rate (documents/second), backend latency per batch, and logs periodic status updates.

**Bypass mode.** When `bypass=true` (test mode), the framework skips your `sendToIndex()` entirely. You don't need to handle test mode in your implementation.

### What you must handle (with guidance from existing implementations)

**Bulk API construction.** Translate Lucille Documents into the search backend's bulk request format. This is backend-specific: Solr uses `SolrInputDocument`, OpenSearch uses `BulkRequest.Builder`, Pinecone uses its own upsert API. See `SolrIndexer.toSolrDoc()`, `OpenSearchIndexer.uploadDocuments()`.

**Per-document failure extraction from bulk responses.** The bulk API may succeed overall but report failures for individual documents. You must parse the response, identify which documents failed, and return them as `Set<Pair<Document, String>>`. See `OpenSearchIndexer` (iterates `BulkResponseItem` checking for errors) and `SolrIndexer` (catches exceptions per-collection).

**Deletion handling.** Detect documents marked for deletion (using `deletionMarkerField`/`deletionMarkerFieldValue`) and issue the appropriate delete operation (delete-by-ID or delete-by-query). The framework provides the config values; you implement the detection logic and the delete API call. See `SolrIndexer.isDeletion()` and `OpenSearchIndexer.isMarkedForDeletion()`.

**Ordering within a batch.** If a batch contains both an upsert and a delete for the same document ID, you must ensure the operations are sent in the correct order. The SolrIndexer handles this by flushing pending upserts before processing a delete for the same ID. The OpenSearch indexer handles it by removing conflicting entries from the upload/delete maps.

**Connection management.** Create the search engine client in the constructor, validate it in `validateConnection()`, close it in `closeConnection()`. Handle TLS, authentication, and certificate validation as needed. See `OpenSearchUtils.getOpenSearchRestClient()` for TLS/auth setup.

**Nested/child document transformation.** If the search backend supports nested documents (Solr does), transform Lucille's attached children into the backend's nested format. See `SolrIndexer.addChildren()`.

**Version type handling.** If the backend supports optimistic concurrency via version numbers (OpenSearch/Elasticsearch do), extract the version from the document (typically the Kafka offset) and include it in the request. See `OpenSearchIndexer`'s `versionType` and `versionNum` logic.

**Retryable vs. non-retryable error classification.** When a bulk call fails, decide whether to throw `IndexerRetryableException` (transient, worth retrying) or `IndexerException` (permanent, don't retry). The distinction is typically based on HTTP status code. See `OpenSearchIndexer` (wraps `OpenSearchException` with status code, wraps `IOException` as unknown status).

---

## Summary: The Division of Labor

| Concern | Framework handles | Implementor handles |
|---|---|---|
| **Batching** | Accumulation, size/timeout flush, MultiBatch | — |
| **Retry** | Exponential backoff, status code filtering | Classifying errors as retryable vs. permanent |
| **Metrics** | Timer, counters, periodic logging | — |
| **Conditional execution** | Evaluating conditions, skipping stages | — |
| **Thread safety** | Per-thread instantiation | Singleton resources (if needed) |
| **Config validation** | SPEC-based pre-run validation | Declaring the SPEC |
| **Document lifecycle tracking** | Publisher accounting, events | — |
| **Backpressure** | maxPendingDocs, queue capacity | — |
| **Error routing** | Catching StageException, sending FAIL events | Deciding what to throw vs. handle |
| **Field filtering** | Whitelist/blacklist application | — |
| **Connection lifecycle** | Calling validate/close at right time | Implementing validate/close |
| **Bulk API** | — | Constructing backend-specific requests |
| **Deletion semantics** | Providing config values | Detecting markers, issuing deletes |
| **Source iteration** | — | Reading from source, creating Documents |
| **Incremental state** | — | Tracking what's been published |
| **Child documents** | Lifecycle tracking, downstream routing | Creating children, assigning IDs |

## Summary

The framework's goal is to let you write the 20% of code that is specific to your source system, your enrichment logic, or your search backend — while the 80% that is common to all search ETL (batching, retry, accounting, metrics, error handling, threading, configuration) is handled once, correctly, in the framework. The opening section of this document describes what that means in practice; the tables above show exactly where the boundaries are.
