---
title: FAQ
weight: 99
date: 2025-06-09
description: >
  Answers to common questions about running, configuring, and extending Lucille.
---

## Setup & Requirements

**What Java version do I need?**

Java 17 or 21. Lucille is compiled targeting Java 17. See the [Support Matrix]({{< relref "docs/operations/support-matrix" >}}) for the full list of tested versions and distributions.

**Do I need Kafka to run Lucille?**

No. Local mode runs entirely in-memory inside a single JVM with no external dependencies beyond the JVM itself. Kafka is only required when running in distributed mode (`-usekafka`). You can develop and test your full pipeline without Kafka and add it later when you need to scale out.

**What search backends are supported?**

Apache Solr, OpenSearch, and Elasticsearch are the supported search backends. The [Support Matrix]({{< relref "docs/operations/support-matrix" >}}) lists the tested versions for each. Pinecone and Weaviate are supported as vector database targets via plugins. A CSV indexer is also available for local development and testing without a running search backend.

Custom connectors and indexers can be added without modifying the core project — implement the relevant interface, place the JAR on the classpath, and reference the fully-qualified class name in your config file.

**What does the `NopIndexer` do?**

It discards all output. Use it in tests and dry runs where you want to exercise the connector and pipeline stages without actually indexing anything. `RunType.TEST` uses a similar mechanism and additionally captures document history for assertion.

---

## Running Lucille

**How do I know when my run is complete?**

In local mode, the `Runner` process exits when the run is complete. In distributed mode, the Runner exits once all documents have reached a terminal state (indexed, failed, or dropped). In both cases, Lucille prints a structured run summary at completion:

```
RUN SUMMARY: Success. 1/1 connectors complete. All published docs succeeded.
connector1: complete. 200000 docs succeeded. 0 docs failed. 0 docs dropped.
```

See [Verifying Your Lucille Run]({{< relref "docs/getting-started" >}}) for details on the log output during and after a run.

**Can I run multiple connectors in one config?**

Yes. The `connectors` block is a list. Each connector runs to completion in the order it is declared before the next one starts. A connector will not start until all documents from the previous connector have been fully indexed. This strict sequencing is intentional — it supports patterns like indexing parent documents before children that reference them.

**Can I run multiple pipelines in one config?**

Yes. Each connector specifies which pipeline it feeds via the `pipeline` parameter. Multiple connectors in the same config can reference different pipelines. All pipelines share the same indexer and search backend.

**What does the `-validate` flag do?**

It runs full configuration validation without executing any ingestion. All component SPECs are checked and all errors are reported at once. Useful in CI pipelines to catch config errors before deployment. The `-render` flag also resolves and prints the config so you can verify what values Lucille will actually use at runtime.

**How do I debug a configuration problem?**

Use `-render` to print the fully resolved config with all environment variable substitutions applied. Use `-validate` to validate all component parameters without running. See [Configuration Management]({{< relref "docs/operations/configuration" >}}).

**Can I pause and resume an ingest?**

Not natively. Lucille runs are bounded: a connector runs to completion or fails. For incremental re-ingestion — processing only files or records that changed since the last run — the `FileConnector` supports an incremental mode. For database ingestion, the `DatabaseConnector` supports parameterized queries that can be scoped to a time window or a watermark.

---

## Pipeline & Stages

**What happens when a Stage throws an exception?**

The document is marked as failed and a FAIL event is sent to the Publisher. The run continues — other documents in the pipeline are not affected. The run summary reports the failure count. To fail the entire run on a document error, set `runner.failOnDocumentError: true`.

**How do I skip a Stage for some documents?**

Use the `conditions` block on the Stage. Conditions can check field presence, field value, or combinations using `conditionPolicy: "all"` or `conditionPolicy: "any"`. A Stage whose conditions are not met is skipped for that document without any error. See [Stages]({{< relref "docs/reference/stages" >}}) for syntax and examples.

**What is the difference between dropping and skipping a document?**

**Dropping** (setting `___dropped = true`) removes the document from the pipeline entirely. It will not be sent to the Indexer. A DROP event is sent to the Publisher. Use this to filter out documents you do not want to index.

**Skipping** (setting `___skipped = true`) bypasses all downstream Stages but the document still reaches the Indexer. The Indexer interprets this as a deletion marker and issues a delete against the search backend for that document's ID. Use this for CDC scenarios where your source emits delete events.

**What is a child document?**

A child document is an additional Document emitted by a Stage that flows through the remaining pipeline stages independently and is indexed as a separate record. A Stage emits children by returning them from `processDocument()` as an `Iterator<Document>`. The Publisher registers each child and tracks it through its own lifecycle.

The most common use case is chunking: a `ChunkText` Stage attaches text chunks to the parent document, and a subsequent `EmitNestedChildren` Stage converts those attached chunks to emitted children that flow through downstream stages and are indexed independently. See [Child Documents]({{< relref "docs/architecture/components/document" >}}).

**Can I write a Stage in Python?**

Two options are available depending on how much Python environment you need.

`EmbeddedPython` runs Python code via an embedded GraalPy interpreter with no external Python installation required. It accepts either an inline `script` string or a `scriptPath` to a file. This is the simpler option for self-contained scripts with no third-party dependencies.

`ExternalPython` manages a CPython virtualenv, installs dependencies from a `requirements.txt`, and calls your Python function per document via Py4J. Use this when your logic depends on PyPI packages (NumPy, spaCy, transformers, etc.).

Constraints that apply to both: only one Python stage configuration can be active per JVM at a time; the function cannot emit child documents; any field absent from the returned dict is removed from the document.

For custom Connectors, Java is required.

---

## Documents

**What field types does Lucille support?**

String, Boolean, Integer, Double, Float, Long, `java.time.Instant`, `byte[]`, `com.fasterxml.jackson.databind.JsonNode`, `java.sql.Timestamp`, and `java.util.Date`. See the [Document]({{< relref "docs/architecture/components/document" >}}) reference for the full API.

**Why does `getString()` return the first value on a multi-valued field instead of throwing?**

By design. A Stage that only needs the primary value of a field does not need to handle the single vs. multi-valued distinction. Use `getStringList()` when you need all values. This pattern — `getString` returns the first, `getStringList` returns all — applies uniformly across all typed getters. See [Document Model]({{< relref "docs/architecture/internals/document-model" >}}) for the rationale.

**How are reserved fields named?**

Reserved internal fields use a triple-underscore prefix: `___dropped`, `___skipped`, `___children`. The `id` and `run_id` fields are also reserved. Stages cannot write to these fields; `validateFieldNames()` enforces this on every write. Reserved fields are stripped by the Indexer before documents reach the search backend.

**How do I control the document ID?**

Set the `id` field when creating the document with `Document.create("my-id")`, or configure an `idField` on the Connector to designate which source field contains the ID. If no ID is provided, a UUID is generated. See [Document IDs]({{< relref "docs/architecture/components/document-ids" >}}).

---

## Distributed Mode & Kafka

**What Kafka version is required?**

Kafka 3.x or 4.x. Lucille uses `kafka-clients` 4.0.0. See the [Support Matrix]({{< relref "docs/operations/support-matrix" >}}).

**What order should I start components in distributed mode?**

ZooKeeper (if using `worker.maxRetries`) → Kafka → Workers → Indexer → Runner. Workers should be consuming before the Runner starts publishing to avoid Kafka consumer group rebalancing delays while documents are already in flight. See [Production Deployment]({{< relref "docs/operations/deployment" >}}).

**What happens if a Worker crashes mid-run?**

Kafka's consumer group protocol detects the failure and reassigns the crashed Worker's partitions to another Worker in the group. Documents that were in flight but not yet acknowledged are redelivered and reprocessed. No documents are silently dropped.

If there was only one Worker and it crashed, there are no remaining Workers to take over its partitions — processing stops until a new Worker is started. The Connector can continue publishing during this window: Kafka keeps accepting messages onto the processing queue regardless of how many Workers are consuming from it. When a Worker comes back up, it picks up from where the crashed Worker left off.

**What is the event topic and why does its name include a run ID?**

The event topic carries FINISH, FAIL, and DROP events from Workers and the Indexer back to the Publisher. Its name includes the run ID so that events from concurrent runs are always isolated — events from run A can never interfere with the Publisher for run B.

This isolation is what makes concurrent runs possible. Worker and Indexer processes are long-running — they stay up between runs and are not restarted by the Runner. Multiple Runner invocations can be active at the same time, all served by the same Worker and Indexer pool. Documents from different runs are interleaved on the Kafka source topic; each document carries its `run_id`, and Workers and Indexers use that `run_id` to route lifecycle events to the correct per-run event topic. Each Runner only consumes its own event topic, so completion accounting stays completely separate across concurrent runs. See [Long-Running Workers and Indexers]({{< relref "docs/operations/deployment" >}}) and [Events]({{< relref "docs/architecture/components/events" >}}).

**Can a WorkerIndexer consume from multiple Kafka topics at once?**

Yes, when running in streaming mode. `WorkerIndexer` interprets `kafka.sourceTopic` as a Java regex pattern, so setting it to a pattern like `"orders_.*_source"` causes the process to consume from all matching topics simultaneously. Kafka's consumer group protocol handles partition assignment across all matched topics and rebalances automatically as new matching topics appear. Note that standalone `Worker` processes subscribe to a single exact topic name and do not support pattern matching. See [Streaming Mode]({{< relref "docs/operations/deployment" >}}).

**Can I use Lucille in streaming mode without a Runner?**

Yes. Start one or more Worker (or WorkerIndexer) processes pointed at a Kafka source topic. An external producer places documents on that topic. Workers consume and process them continuously with no run boundary or completion accounting. Set `kafka.events: false` if you do not need event tracking. See [Streaming Mode]({{< relref "docs/operations/deployment" >}}).

---

## Plugins & Extensions

**What plugins are available?**

The `lucille-plugins` module contains: `lucille-tika` (text extraction from 1000+ file formats), `lucille-ocr` (Tesseract OCR), `lucille-entity-extraction` (OpenNLP NER), `lucille-jlama` (local LLM embeddings with no external API), `lucille-parquet` (Parquet file support), `lucille-pinecone` (Pinecone indexer), `lucille-weaviate` (Weaviate indexer), `lucille-video` (video frame extraction), and `lucille-api` (REST API for run triggering). Plugins are separate Maven modules that do not bloat the core JAR.

**When should a component go in `lucille-core` vs. a plugin?**

Add to `lucille-core` if the component is general-purpose and has no heavy transitive dependencies. Create a plugin module if the component depends on a large library, would introduce transitive dependency conflicts, or is specialized enough that most users do not need it. See [Quick Reference]({{< relref "docs/developer-guide/quick-reference" >}}).

**How do I write a new Stage?**

Implement the `Stage` interface: override `start()` for initialization, `processDocument()` for per-document logic, and `stop()` for cleanup. Declare a `public static final Spec SPEC` describing all configuration parameters. See [Developing New Components]({{< relref "docs/developer-guide/dev_new_components" >}}).

---

## Troubleshooting

**My run is hanging — it started but never completes.**

Check the periodic log messages. If "Waiting on N docs" is not decreasing, documents are stuck somewhere. Common causes:
- The Indexer can't reach the search backend (check for connection errors in the Indexer thread's logs).
- A Worker is stuck on a single document (check for "Worker has not polled in N seconds" warnings from the WorkerWatcherExecutorService).
- An event was lost (extremely rare — look for "RUN WILL HANG" in the logs).
- The connector timeout hasn't been reached yet (default: 24 hours). Set `runner.connectorTimeout` to a shorter value if appropriate.

See [Log Inspection and Analysis]({{< relref "docs/operations/log-analysis" >}}) for how to diagnose from log output.

**How do I trace a specific document through the system?**

Enable the DocLogger at INFO level in your log4j2 configuration and route it to a file. Every significant transition a document makes is logged with the document ID in the MDC. You can then grep the log file for a specific document ID to see its complete history: publication, each stage entry/exit, indexer receipt, and final FINISH or FAIL event. See [Logging]({{< relref "docs/operations/logging" >}}).

**Can I run multiple independent pipelines in parallel for faster wall-clock time?**

Yes. Launch separate Runner processes with separate config files — each gets its own run_id and runs independently. Orchestrate with a simple shell script. The tradeoff: you can't correlate all documents under a single run_id, and you need to ensure document IDs don't collide if pipelines write to the same index (use `docIdPrefix`). See [Parallelizing Multiple Pipelines]({{< relref "docs/operations/performance-tuning" >}}).

**My run completes but documents aren't visible in the search backend.**

For Solr, a commit is required to make documents visible. Issue a commit with `openSearcher=true` after indexing completes. For Elasticsearch and OpenSearch, documents are available after the refresh interval (default 1 second). Use the `-validate` flag to confirm configuration is correct before running.

**How do I inspect what documents look like mid-pipeline, or replay a pipeline run without re-running enrichment?**

The `Print` stage logs documents as JSON at any point in the pipeline and can write them to a JSONL file. Combined with a `NopIndexer`, this lets you capture fully-enriched documents to disk without indexing anything. You can then replay that file using `FileConnector` with the JSON handler and an empty pipeline, sending the already-processed documents directly to a live search backend. This is useful for iterating on indexer configuration or field mappings without repeating expensive enrichment steps (OCR, embeddings, database lookups). See the Print stage entry in [All Stages]({{< relref "docs/reference/stages/stages_reference" >}}) for the full pattern.

**A Stage is initializing a large model for every document instead of once per thread.**

Put initialization in `start()`, not in `processDocument()`. `start()` is called once per Worker thread when the pipeline is initialized. Resources initialized there are reused for every document that thread processes. See [Quick Reference]({{< relref "docs/developer-guide/quick-reference" >}}).

**Lucille is running out of memory.**

Set `-Xmx` explicitly — Lucille will otherwise use all available JVM heap. In local mode, set `publisher.queueCapacity` to bound the number of in-flight documents. In distributed mode, set `publisher.maxPendingDocs`. See [Memory Sizing]({{< relref "docs/operations/deployment" >}}).

**How do I report a bug or request a feature?**

Open an issue on [GitHub](https://github.com/kmwtechnology/lucille/issues).
