---
title: "Design Rationale (v2)"
weight: 8
date: 2025-06-09
description: >
  The guiding principles and detailed requirements that govern Lucille's architecture.
---

This page documents the design decisions behind Lucille in two parts: the **guiding principles** that define what kind of system Lucille is, and the **requirements** that specify what the system must do. The principles emerged from years of building search ingestion frameworks for customer projects. The requirements evolved through production deployments and were refined through real-world feedback.

---

## Guiding Principles

These are the overarching design decisions that shape everything else. Every architectural choice, every API design, and every operational feature must be consistent with these principles.

### 1. Component separation

The three tasks of retrieving data from a source system, processing it through an enrichment pipeline, and sending it to the search engine are handled by separate components: Connector, Worker, and Indexer. These components run concurrently and communicate through queues. No component waits for another unless it exhausts its backlog of work or the downstream queue is full.

This separation exists because the three tasks have fundamentally different performance characteristics — reading is I/O-bound, enrichment is CPU-bound, and indexing involves network round-trips with batching semantics. Running them concurrently allows each to operate at its natural pace, and the system achieves optimal throughput given available resources.

### 2. Scalability

It should be easy to scale the system by adding more component instances — especially more Workers — even while the system is running. In distributed mode, adding a Worker process to a running ingest requires no restart and no coordination: the new Worker joins the Kafka consumer group, receives partition assignments, and begins processing immediately.

Within a single process, adding Worker threads is a configuration change (`worker.threads`). Across processes, adding Workers is launching another JVM. Both are independent scaling levers: threads for vertical scaling within a machine, processes for horizontal scaling across machines.

### 3. Extensibility

It should be easy to extend the system by adding new implementations of the core component types — new Connectors for new data sources, new Stages for new enrichment operations, new Indexers for new search backends — without modifying the framework itself.

This is achieved through small, well-defined interfaces (Stage, Connector, Indexer), reflective instantiation from class names in configuration, and a modular Maven project structure where plugins can be added as separate modules. The classpath is the plugin mechanism: put your JAR on the classpath, reference the class by name in config, and it works.

### 4. Deployment expandability

There should be a simple, self-contained deployment mode running in a single JVM (local mode) as well as a fully distributed mode with separate processes communicating through Kafka. The system should also support a hybrid mode (WorkerIndexer) that pairs a Worker and Indexer in a single process for horizontal scaling without the operational complexity of managing them separately.

Critically, the codepaths remain nearly identical when switching from one deployment mode to another. The same pipeline configuration, the same Stage implementations, the same Connector code, and the same Indexer logic run in all modes. Only the messaging implementation changes (in-memory queues vs. Kafka). This means a pipeline developed and tested in local mode will behave the same way in distributed mode — the transition is a command-line flag, not a code change.

### 5. Batch and streaming agnosticism

The system should support both batch architecture (a finite set of data retrieved, processed, and indexed, with clear completion detection) and streaming architecture (an unbounded supply of documents arriving continuously with no run boundary).

A Stage that extracts entities or generates embeddings does not know or care which mode it is running under. Pipeline logic is written once and deployed in either mode. For real-world search systems — which typically need a historical backfill (batch) followed by continuous updates (streaming) — this means one pipeline definition, not two.

### 6. Testability

With mocking of external systems (the source and the search backend), the actual Lucille framework should easily run inside a unit test. The system provides a dedicated test mode (`RunType.TEST`) that runs the full pipeline end-to-end with in-memory messaging and a bypassed indexer, capturing the complete history of every document for assertion.

Testing a pipeline requires no external infrastructure — no Kafka, no search engine, no database. The framework runs as it normally does, right inside the test. The mocking focuses on external services; the Lucille components themselves are real.

### 7. Concurrency without burden

While the system is highly concurrent (multiple Worker threads, asynchronous components, queue-based communication), users of the system — pipeline authors writing Stages and Connectors — should not have to worry about concurrency.

This is achieved through per-thread Pipeline instantiation: each Worker thread gets its own Pipeline with its own instances of every Stage. Instance fields in a Stage are effectively thread-local. A Stage author writes sequential code — read a field, transform it, write a field — and the framework handles parallelism. No synchronization, no locks, no concurrent data structures required in stage implementations.

### 8. Maximum throughput

The system should allow your ingest to run as fast as possible given available system resources and the latency of external systems. The framework itself is never the bottleneck.

This means: lazy iterator-based pipeline processing (bounded memory regardless of child document count), zero-cost JSON serialization (the Document is already in wire format), batched indexing (amortizing network round-trips), backpressure (preventing out-of-memory conditions without artificial throttling), and the ability to scale Workers to saturate available CPU. The bottleneck should always be the source system, the enrichment logic, or the search backend — never the framework's own overhead.

### 9. Configuration-driven operation

All aspects of an ingest — which connectors to run, which pipelines to use, which stages to apply, connection details, tuning parameters — should be specified in a readable configuration file. Changes should be possible by editing the file alone, without rebuilding the project.

Configuration should be composable (reusable across files via includes), parameterizable (environment variable substitution for credentials and environment-specific values), and validatable (errors caught before execution starts, all at once rather than one at a time).

### 10. Optimistic error handling

The system should ingest as much data as possible, continuing past per-document errors rather than aborting. A search index is often useful even if it doesn't contain 100% of the data from the source system. Source systems routinely contain messy data where some records will inevitably cause errors in processing.

However, the system should stop immediately on structural errors — invalid configuration, failed Stage initialization, unreachable search backend — rather than wasting time and resources on work that cannot succeed. The guiding principle: get as much data into the search engine as possible, but stop as soon as possible if you'd only be wasting your time.

---

## Requirements

These are the specific capabilities the system must provide. They are derived from the guiding principles and refined through production deployments.

**1. Completion detection.** In the batch execution model, the system must provide a clear indication of when all work is complete. This is complex in a distributed framework with multiple Indexer instances running separately from the Connector, each asynchronously completing items of work, and where new work can be generated inside Workers. Lucille solves this with an event-driven accounting system: the Publisher tracks work sent into the system, Workers notify the Publisher when additional work is generated, and Indexers notify the Publisher when work is completed.

**2. Child documents.** The system must support "child" Documents created inside the processing Pipeline. While most work is generated by the Connector, a Worker can also generate new work — for example, splitting a zip file into its component files, or chunking a document's text into embedding-sized pieces. Each child flows through downstream Stages and is indexed as its own record, tracked independently by the accounting system.

**3. Sequential composition.** Multiple batch ingests can be composed in strict sequence — one ingest cannot start until all work from the previous ingest is complete. This supports patterns like indexing parent documents before child documents that reference them.

**4. Streaming without a Connector.** In streaming mode, Workers can consume directly from a Kafka topic populated by an external system, with no Runner or Publisher. The accounting system can be disabled when not needed.

**5. Observability.** Each component reports metrics about its performance continuously during ingestion, with summaries at the end. Components provide a heartbeat mechanism for container orchestrators to verify liveness.

**6. Per-document error handling.** Exceptions during Document processing fail the individual document without stopping the ingest. The document is routed to a failure state; other documents continue flowing.

**7. Poison pill detection.** Documents that repeatedly cause a Worker process to crash are detected (via a distributed retry counter backed by ZooKeeper or Redis) and routed to a Dead Letter Queue after a configurable retry limit.

**8. Crash resilience.** Work should not be lost if a distributed Worker or Indexer crashes during an ingest. Uncompleted or in-progress work is resumed when the component is restarted, or picked up by another available instance via Kafka's consumer group protocol.

**9. Flexible Document model.** The Document API must make common manipulations easy and concise — typed field access, single-valued and multi-valued fields handled uniformly, update modes (overwrite, append, skip) matching how search engines actually work, and zero-cost serialization to JSON.

**10. Operation ordering.** Sequences of creates, updates, and deletes for the same document must preserve their order even when running in distributed mode with multiple Workers and Indexers. This is achieved by using the document ID as the Kafka message key, ensuring all operations for the same document land on the same partition and are consumed sequentially.

**11. Batch indexing.** Indexers send documents to the search backend in configurable batches with both size and timeout thresholds. The timeout ensures documents are flushed with bounded latency even in low-volume scenarios.

**12. Connector lifecycle.** Connectors have pre-execution, execution, post-execution, and close phases with defined error semantics — postExecute is not called if execute fails; close is always called regardless of outcome.

**13. Document collapsing.** When a Connector emits multiple consecutive Documents with the same ID (common in CDC scenarios), the Publisher can merge them into a single Document with multi-valued fields before sending to the pipeline.

**14. Conditional stage execution.** Stages execute only when the document meets criteria specified in configuration (field presence, field value, combinations with all/any policy). This is handled by the framework — stage authors never implement conditional logic.

**15. Inspectable data in flight.** Documents on Kafka topics are human-readable JSON. An administrator can inspect the source, destination, and event topics with standard Kafka tooling and understand what is in the system at any point.

**16. Source coverage.** Built-in connectors for databases (JDBC), filesystems (local, S3, Azure, GCS), CSV/XML/JSON files, Kafka topics, RSS feeds, and search engines (Solr).

**17. Multiple search backends.** Solr, OpenSearch, Elasticsearch, Pinecone, Weaviate, and CSV are supported as indexing destinations, with and without security enabled.

**18. Backpressure.** The Publisher blocks the Connector when too many documents are in flight (`maxPendingDocs`) or when queues are full (`queueCapacity`), preventing out-of-memory conditions without requiring the user to manually throttle the Connector.

**19. ID override at indexing time.** The document's ID in the search backend can differ from its internal tracking ID (`idOverrideField`), decoupling pipeline accounting from index identity.

**20. Index routing.** Documents in the same batch can be routed to different indices or collections (`indexOverrideField`), supporting multi-tenant and multi-index architectures.

**21. Deletion support.** Documents can represent delete-by-ID or delete-by-query operations via configurable marker fields, enabling CDC and incremental ingestion patterns.

**22. Document ID prefixing.** Connectors can namespace their document IDs (`docIdPrefix`) to prevent collisions when multiple connectors write to the same index.

**23. Programmatic run triggering.** Runs can be triggered and managed via a REST API (RunnerManager), with support for concurrent runs in the same JVM.

**24. Configuration rendering and validation.** The fully resolved config can be printed for debugging (`-render`) and validated without execution (`-validate`), catching all errors at once before any work begins.

**25. Incremental/stateful ingestion.** Connectors can track what they have previously published (via JDBC-backed state) and process only new or modified data on subsequent runs.

**26. Multi-cloud file access.** A single FileConnector handles local filesystem, S3, Azure Blob, and GCS through pluggable StorageClient implementations selected by URI scheme.

**27. Indexer retry with exponential backoff.** Failed batch calls are retried with configurable maximum attempts, initial wait duration, and retryable status codes. Non-retryable failures fail immediately.

**28. Graceful shutdown.** Signal handling (SIGINT/SIGTERM) cleanly stops all components — the Connector stops publishing, Workers drain remaining documents, the Indexer flushes its current batch, and the process exits with a run summary.

**29. Built-in enrichment stages.** Common processing tasks are available out of the box: field manipulation, regex, text operations, entity extraction, language detection, embeddings, chunking, database lookups, HTTP enrichment, scripting (JavaScript, Python), and JSONata transformations.
