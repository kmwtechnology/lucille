---
title: "Design Rationale (v4)"
weight: 10
date: 2025-06-09
description: >
  The guiding principles that govern Lucille's architecture, and the features that achieve them.
---

These are the overarching design decisions that shape everything about Lucille. Every architectural choice, every API design, and every operational feature must be consistent with these principles. They emerged from years of building search ingestion frameworks for customer projects and were refined through production deployments.

---

## Part I: Guiding Principles

### 1. Built for search

The system should be purpose-built for getting data into search engines and vector databases — not a general-purpose ETL tool adapted for search. This focus should shape the document model, the identity model, the wire format, and the operations the system supports natively.

### 2. Concurrency

The tasks of retrieving data, processing it, and sending it to the search engine should be handled by separate components that run concurrently. No component should wait for another unless it exhausts its backlog of work. While the system should be highly concurrent, pipeline authors should not have to manage concurrency — the framework should handle parallelism transparently.

### 3. Scalability

It should be easy to scale the system by adding more component instances — especially more Workers — even while the system is running. Scaling up should not require stopping the ingest, modifying configuration, or redeploying.

### 4. Extensibility

It should be easy to extend the system by adding new implementations of the core component types — new Connectors, new Stages, new Indexers — without modifying the framework itself. Extension points should be defined by small, well-documented interfaces.

### 5. Deployment versatility

The system should be easy to deploy as a single, self-contained process, and it should have a clear pathway for expanding into a distributed deployment. The codepaths should remain nearly identical when switching from one deployment mode to another.

### 6. Batch and streaming unity

The system should support both batch architecture (finite data with completion detection) and streaming architecture (unbounded data with no run boundary). Enrichment logic should not know or care which mode it is running under.

### 7. Minimal framework overhead

The framework itself should never be the bottleneck. Per-document overhead should be negligible relative to the actual enrichment work. Available system resources should be used effectively, with the constraint always being the source system, the enrichment logic, or the search backend — never the framework.

### 8. Observability

The system should make it easy to understand what is happening during an ingest, what has happened after an ingest, and what went wrong when something fails. Metrics, logging, and status reporting should be built into the framework, not bolted on.

### 9. Testability

The entire framework should easily run inside a unit test. It should be straightforward to write end-to-end tests of ingestion pipelines that exercise the real framework components with only external systems mocked or bypassed.

### 10. Configuration-driven operation

All aspects of an ingest should be specified in a readable configuration file. Changes should be possible by editing the file alone, without rebuilding the project. Configuration should be composable, parameterizable, and validatable.

### 11. Optimistic error handling

The system should ingest as much data as possible, continuing past per-document errors rather than aborting. However, the system should stop immediately on structural errors rather than wasting time on work that cannot succeed.

### 12. Resilience

The system should recover from failures without losing work. Transient failures should be retried, poison-pill documents should be quarantined, and the system should shut down gracefully when asked, preserving in-flight work rather than abandoning it.

---

## Part II: Features by Principle

### 1. Built for search

The document model represents data the way search engines think about it: typed fields, single-valued and multi-valued, with operations that map directly to search engine semantics.

- **Search-oriented document model.** Fields support single and multi-valued access uniformly. The three update modes (overwrite, append, skip) match how search engine fields are actually populated.
- **Deterministic document IDs.** Connectors derive IDs from source data (file paths, database primary keys, URLs) so that re-ingestion updates existing records rather than creating duplicates.
- **ID namespacing.** A configurable `docIdPrefix` on each Connector prevents ID collisions when multiple connectors write to the same index.
- **ID override at indexing time.** The `idOverrideField` setting lets the Indexer use a different field's value as the document's ID in the search backend, decoupling internal tracking from index identity.
- **Deletion markers.** Documents can represent delete-by-ID or delete-by-query operations via configurable marker fields, enabling CDC and incremental ingestion patterns.
- **Operation ordering.** Sequences of creates, updates, and deletes for the same document ID preserve their order in distributed mode by using the document ID as the Kafka message key.
- **Zero-cost JSON serialization.** The document is backed by a Jackson ObjectNode — already in the wire format that search engine bulk APIs expect. No conversion at the boundary.
- **Multiple search backends.** Solr, OpenSearch, Elasticsearch, Pinecone, Weaviate, and CSV are supported as indexing destinations.
- **Index routing.** Documents in the same batch can be routed to different indices or collections via `indexOverrideField`, supporting multi-tenant architectures.

### 2. Concurrency

Connector, Worker, and Indexer run as independent concurrent components communicating through queues. The framework manages all parallelism so that pipeline authors write sequential code.

- **Component separation.** Each component operates at its own pace. The Connector publishes to a queue, Workers pull and process, the Indexer batches and sends. No component blocks another unless queues are full or empty.
- **Per-thread pipeline instantiation.** Each Worker thread gets its own Pipeline with its own instances of every Stage. Instance fields in a Stage are effectively thread-local — no synchronization required.
- **Event-driven completion detection.** The Publisher tracks every document from publication to terminal state (FINISH, FAIL, DROP) using an event queue. The run is complete when all documents reach a terminal state.
- **Bag-based accounting.** The Publisher uses a multiset (Bag) rather than a Set, correctly handling duplicate IDs published in the same run — each publication requires its own terminal event.
- **Concurrent run isolation.** Each batch run gets a dedicated event topic (`{pipeline}_event_{runId}`), preventing events from one run interfering with another.
- **Child documents.** Stages can generate additional documents mid-pipeline. The Publisher is notified via CREATE events and tracks children independently.
- **Sequential composition.** Multiple batch ingests can be composed in strict sequence — one ingest cannot start until all work from the previous ingest is complete.
- **Document collapsing.** The Publisher can merge consecutive same-ID documents into a single document before sending for processing, reducing redundant work in CDC scenarios.

### 3. Scalability

Adding capacity is an operational action, not a development effort.

- **Hot scaling.** New Worker instances can join a running ingest without restart or coordination. In distributed mode, a new Worker joins the Kafka consumer group and receives partition assignments immediately.
- **Vertical scaling.** Within a single process, adding Worker threads is a configuration change (`worker.threads`).
- **Horizontal scaling.** Across processes, adding Workers means launching additional JVMs. Both are independent levers.
- **WorkerIndexer hybrid.** Co-located Worker+Indexer processes provide horizontal scaling without the operational complexity of managing separate fleets.

### 4. Extensibility

New components are added by implementing an interface, placing the compiled code on the classpath, and referencing it by name in configuration.

- **Interface-based extension.** Stage, Connector, and Indexer are small, well-defined interfaces. Implementing one is the only requirement for adding new functionality.
- **Runtime discovery.** Components are instantiated reflectively from class names in configuration. No registration step, no framework modification, no core rebuild required.
- **Modular Maven structure.** Plugins are separate Maven modules. New connectors, stages, and indexers can be developed and versioned independently of the core.
- **Built-in enrichment stages.** Common processing tasks are available out of the box: field manipulation, regex, text operations, entity extraction, language detection, embeddings, chunking, database lookups, HTTP enrichment, scripting (JavaScript, Python), and JSONata transformations.
- **Source coverage.** Built-in connectors for databases (JDBC), filesystems (local, S3, Azure, GCS), CSV/XML/JSON files, Kafka topics, RSS feeds, and search engines.
- **Multi-cloud file access.** A single FileConnector handles local filesystem, S3, Azure Blob, and GCS through pluggable StorageClient implementations selected by URI scheme.

### 5. Deployment versatility

The same pipeline configuration, the same component implementations, and the same enrichment logic run regardless of deployment mode.

- **Local mode.** All components run as threads in a single JVM with in-memory queues. No external infrastructure required.
- **Distributed mode.** Components run as separate processes communicating via Kafka topics.
- **Hybrid mode (WorkerIndexer).** Worker and Indexer co-located in a single process, consuming from Kafka but avoiding a second Kafka round-trip between processing and indexing.
- **Streaming mode.** Workers consume directly from a Kafka topic populated by an external system, with no Runner or Connector.
- **Deployment-independent codepaths.** Only the messaging implementation changes between modes (in-memory queues vs. Kafka). All other code paths — pipeline stages, accounting logic, retry behavior, batching — remain identical.
- **Programmatic run triggering.** Runs can be triggered and managed via a REST API (RunnerManager), with support for concurrent runs in the same JVM.

### 6. Batch and streaming unity

Pipeline logic is written once and deployed in either mode without modification.

- **Batch mode.** A Runner triggers a finite ingest with completion detection. The Connector retrieves a bounded set of data, and the system reports when all work is done.
- **Streaming mode.** An external system places documents on a Kafka topic continuously. Workers process them with no run boundary or completion accounting.
- **Mode-transparent stages.** A Stage that extracts entities or generates embeddings works identically in both modes. It never knows which mode it is running under.
- **Incremental/stateful ingestion.** Connectors can track what they have previously published (via JDBC-backed state) and process only new or modified data on subsequent runs — bridging batch and streaming patterns.

### 7. Minimal framework overhead

The bottleneck should always be the source system, the enrichment logic, or the search backend — never the framework's own overhead.

- **Zero-cost JSON serialization.** The Document is already in wire format. No conversion step at queue boundaries or when sending to search backends.
- **Lazy iterator-based processing.** Child documents are produced as an iterator, not materialized into a list. Memory usage is bounded regardless of how many children a Stage produces.
- **Batch indexing.** Documents are sent to the search backend in configurable batches with both size and timeout thresholds, amortizing network round-trips.
- **Backpressure.** The Publisher blocks the Connector when too many documents are in flight, preventing out-of-memory conditions without artificial throttling or unbounded queue growth.

### 8. Observability

Understanding system behavior requires no custom instrumentation by the user.

- **Per-component metrics.** Each component reports throughput and latency continuously during execution via a shared MetricRegistry.
- **Per-stage timing.** Processing time is measured per Stage, so pipeline bottlenecks can be identified without adding instrumentation to stage code.
- **Run summary.** A structured summary at the end of each run shows documents succeeded, failed, dropped, and total elapsed time.
- **Inspectable data in flight.** Documents on Kafka topics are human-readable JSON. An administrator can inspect any topic with standard Kafka tooling.
- **Per-document tracing.** The run ID and document ID are pushed into the SLF4J MDC, so every log line emitted while processing a document includes its identity.
- **Heartbeat/liveness.** Components report liveness for container orchestrators to verify health.

### 9. Testability

Testing a pipeline requires no external infrastructure — no Kafka, no search engine, no database.

- **Test mode (RunType.TEST).** The full pipeline runs end-to-end with in-memory messaging and a bypassed indexer. The real framework components execute — real queues, real pipeline processing, real document routing.
- **Complete document history.** A TestMessenger captures every document published, every document sent for indexing, and every lifecycle event. Test code asserts against this history.
- **No external infrastructure.** Tests run with in-memory queues. The mocking focuses on external services (source systems, search backends); the Lucille components themselves are real.

### 10. Configuration-driven operation

Changes are possible by editing a configuration file alone, without rebuilding the project.

- **HOCON format.** Comments, relaxed syntax, and human-readable structure. Any valid JSON is also valid HOCON.
- **Environment variable substitution.** Credentials and environment-specific values are injected via `${?ENV_VAR}` syntax without code changes.
- **Config composition (includes).** Shared settings — connection strings, common pipeline fragments — are defined once and included everywhere.
- **Pre-run validation (SPEC system).** Every component declares its expected configuration. All errors are reported at once before execution starts, so a developer can fix all issues in a single pass.
- **Config rendering (-render).** The fully resolved config can be printed for debugging, showing what values Lucille will actually see at runtime.
- **Validation without execution (-validate).** Configuration can be checked in CI pipelines without running an ingest.
- **Conditional stage execution.** Stages execute only when the document meets criteria specified in configuration (field presence, field value, combinations). The framework handles this — stage authors never implement conditional logic.
- **Connector lifecycle.** Connectors have pre-execution, execution, post-execution, and close phases with defined error semantics.

### 11. Optimistic error handling

The guiding principle: get as much data into the search engine as possible, but stop as soon as possible if you'd only be wasting your time.

- **Per-document error handling.** Exceptions during document processing fail the individual document without stopping the ingest. The document is routed to a failure state; other documents continue flowing.
- **Structural error fast-fail.** Invalid configuration, failed component initialization, or an unreachable search backend cause immediate termination rather than processing documents that can never be indexed.

### 12. Resilience

The system should recover from failures without losing work and prevent resource exhaustion proactively.

- **Poison pill detection.** Documents that repeatedly cause a Worker process to crash are detected (via a distributed retry counter) and routed to a Dead Letter Queue after a configurable retry limit.
- **Crash resilience.** Uncompleted work is resumed when a component restarts, or picked up by another instance via Kafka's consumer group protocol. Work is not silently dropped.
- **Graceful shutdown.** Signal handling (SIGINT/SIGTERM) cleanly stops all components — the Connector stops publishing, Workers drain remaining documents, the Indexer flushes its current batch, and the process exits with a run summary.
- **Backpressure.** The Publisher blocks the Connector when too many documents are in flight or when queues are full, preventing unbounded growth that leads to out-of-memory crashes.
- **Indexer retry with exponential backoff.** Failed batch calls are retried with configurable maximum attempts, initial wait duration, and retryable status codes. Non-retryable failures fail immediately.
