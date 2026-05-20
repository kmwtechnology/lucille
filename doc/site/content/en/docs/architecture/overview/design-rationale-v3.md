---
title: "Design Rationale (v3)"
weight: 9
date: 2025-06-09
description: >
  The guiding principles that govern Lucille's architecture.
---

These are the overarching design decisions that shape everything about Lucille. Every architectural choice, every API design, and every operational feature must be consistent with these principles. They emerged from years of building search ingestion frameworks for customer projects and were refined through production deployments.

---

## Guiding Principles

### 1. Built for search

The system should be purpose-built for getting data into search engines and vector databases — not a general-purpose ETL tool adapted for search. This focus should shape every design decision.

The document model should represent data the way search engines think about it: typed fields, single-valued and multi-valued, with operations (create, update, delete) that map directly to search engine semantics. Document identity should be stable and deterministic so that re-ingestion updates existing records rather than creating duplicates. The system should support ID namespacing across sources, ID override at indexing time, and deletion markers that translate to search backend delete operations. The wire format should be the format search APIs expect, minimizing conversion at the boundary.

### 2. Concurrency

The three tasks of retrieving data from a source system, processing it through an enrichment pipeline, and sending it to the search engine should be handled by separate components — Connector, Worker, and Indexer — that run concurrently. Each component should operate at its own pace. No component should wait for another unless it exhausts its backlog of work or the downstream queue is full.

While the system should be highly concurrent, users of the system should not have to worry about managing concurrency. Pipeline authors should write sequential code — read a field, transform it, write a field — and the framework should handle parallelism transparently. The complexity of concurrent execution should be the framework's responsibility, not the user's.

### 3. Scalability

It should be easy to scale the system by adding more component instances — especially more Workers — even while the system is running. Scaling up should not require stopping the ingest, modifying configuration, or redeploying. A new Worker instance should join the system and begin processing immediately.

Within a single process, scaling should mean adding threads. Across processes, scaling should mean launching additional instances. Both should be independent levers that can be tuned based on hardware and workload characteristics.

### 4. Extensibility

It should be easy to extend the system by adding new implementations of the core component types — new Connectors for new data sources, new Stages for new enrichment operations, new Indexers for new search backends — without modifying the framework itself.

Extension points should be defined by small, well-documented interfaces. New components should be added by implementing an interface, placing the compiled code on the classpath, and referencing it by name in configuration. The framework should discover and instantiate components at runtime. No registration step, no framework modification, no rebuild of the core project should be required.

### 5. Deployment versatility

The system should be easy to deploy as a single, self-contained process, and it should have a clear pathway for expanding a single-process deployment into a distributed deployment where the components run in separate dedicated processes.

The codepaths should remain nearly identical when switching from one deployment mode to another. The same pipeline configuration, the same component implementations, and the same enrichment logic should run regardless of whether the system is deployed as one process or many. The transition from simple to distributed should be an operational decision, not a development effort.

### 6. Batch and streaming unity

The system should support both batch architecture — a finite set of data retrieved, processed, and indexed, with clear completion detection — and streaming architecture — an unbounded supply of documents arriving continuously with no run boundary.

Enrichment logic should not know or care which mode it is running under. A Stage that extracts entities or generates embeddings should work identically in both modes. Pipeline definitions should be written once and deployed in either mode without modification.

### 7. Minimal framework overhead

The system should allow your ingest to run as fast as possible given available system resources and the latency of external systems. The framework itself should never be the bottleneck.

When the source system can produce data faster than the pipeline can process it, the pipeline should be the constraint — not the framework's overhead. When the pipeline can process faster than the search backend can accept, the backend should be the constraint. The framework's job should be to get out of the way: minimize per-document overhead, avoid unnecessary copying or serialization, and ensure that available CPU, memory, and I/O bandwidth are used effectively.

### 8. Observability

The system should make it easy to understand what is happening during an ingest, what has happened after an ingest, and what went wrong when something fails. Metrics, logging, and status reporting should be built into the framework, not bolted on.

Each component should report its throughput and latency continuously during execution. Per-stage performance should be measurable so that bottlenecks can be identified without instrumentation by the user. The system should provide a structured summary at the end of each run showing what succeeded, what failed, and how long it took. Data in flight should be inspectable with standard tooling. And per-document lifecycle events should be traceable — it should be possible to follow a single document's journey through the entire system from publication to indexing.

### 9. Testability

The entire framework should easily run inside a unit test. It should be straightforward to write end-to-end tests of ingestion pipelines that exercise the real framework components — real queues, real pipeline execution, real document routing — with only external systems (the data source and the search backend) mocked or bypassed.

Testing a pipeline should require no external infrastructure. The system should provide a test mode that captures the complete history of every document for assertion, making it possible to verify correctness without deploying anything beyond the test itself.

### 10. Configuration-driven operation

All aspects of an ingest — which connectors to run, which pipelines to use, which stages to apply, connection details, tuning parameters — should be specified in a readable configuration file. Changes should be possible by editing the file alone, without rebuilding the project.

Configuration should be composable — reusable across files via includes, so that shared settings (connection strings, common pipeline fragments) are defined once and referenced everywhere. It should be parameterizable — supporting environment variable substitution for credentials and environment-specific values. And it should be validatable — errors caught before execution starts, all at once rather than one at a time, so that a developer can fix all configuration issues in a single pass.

### 11. Optimistic error handling

The system should ingest as much data as possible, continuing past per-document errors rather than aborting. A search index is often useful even if it doesn't contain 100% of the data from the source system. Source systems routinely contain messy data where some records will inevitably cause errors in processing. Stopping the entire ingest because one document out of millions is problematic would be counterproductive.

However, the system should stop immediately on structural errors — invalid configuration, failed component initialization, unreachable search backend — rather than wasting time and resources on work that cannot succeed. The guiding principle: get as much data into the search engine as possible, but stop as soon as possible if you'd only be wasting your time.

### 12. Resilience

The system should recover from failures without losing work. If a distributed component crashes mid-ingest, uncompleted work should be resumed by another instance — not silently dropped. Documents that repeatedly cause crashes should be detected and quarantined rather than allowed to block the entire ingest indefinitely. Transient failures (network timeouts, rate limits) should be retried with appropriate backoff rather than treated as permanent.

The system should shut down gracefully when asked, preserving in-flight work rather than abandoning it. And it should prevent resource exhaustion proactively — blocking producers when consumers fall behind, rather than allowing unbounded growth that leads to crashes.
