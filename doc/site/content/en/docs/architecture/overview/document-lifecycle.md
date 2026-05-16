---
title: "Document Lifecycle"
weight: 3
date: 2025-06-09
description: >
  The complete journey of a single Document through Lucille, from creation to indexing, plus cross-cutting concerns.
---

## Part 1: The Architecture

Lucille separates the three concerns of ETL — reading, transforming, and writing — into four concurrent components that communicate through queues.

**Connector** reads from a source system and emits Documents. It runs in its own thread, driven by a `ConnectorThread`.

**Worker(s)** pull Documents from the source queue, run them through a Pipeline of Stages, and push results to the destination queue. Multiple Worker threads can run concurrently, each with its own Pipeline instance.

**Indexer** pulls processed Documents from the destination queue and sends them to the search backend in batches. It runs in its own thread.

**Publisher** tracks every Document from the moment it enters the system until it reaches a terminal state (indexed, failed, or dropped). It runs on the main thread, polling an event queue. The Connector uses the Publisher to submit Documents; the Worker and Indexer use it to report status changes via Events.

The queues are the only coupling between components. In local mode they are `LinkedBlockingQueue` instances in memory. In distributed mode they are Kafka topics. The component code is identical in both cases — only the messenger implementation changes.

---

## Part 2: The Happy Path

This is the complete journey of a single Document, with edge cases omitted.

**1. Runner startup.** `Runner.run()` validates the full configuration, then for each Connector: starts a `WorkerPool` (N Worker threads), starts an Indexer thread, creates a `PublisherImpl`, and launches the Connector in a `ConnectorThread`. The main thread then calls `publisher.waitForCompletion()` and blocks.

**2. Document creation.** Inside `connector.execute(publisher)`, the Connector reads a record from its source, creates a `Document` with a user-visible ID, and calls `publisher.publish(document)`.

**3. Publication.** The Publisher stamps the document with the run ID, registers the document's ID in its accounting ledger, and places the document on the source queue.

**4. Pipeline processing.** A Worker thread picks up the document, passes it through each Stage in sequence, and receives back an iterator of result documents. For a simple document with no children, this is just the modified document itself.

**5. Indexer batching.** The Worker places the processed document on the destination queue. The Indexer accumulates it in a batch. When the batch reaches its size or timeout threshold, the Indexer sends the batch to the search backend in a single bulk API call.

**6. Completion accounting.** The Indexer sends a FINISH Event to the event queue. The Publisher's polling loop receives it, removes the document's ID from its ledger, and decrements the pending count.

**7. Run completion.** When the Connector thread finishes, the source queue drains, the Workers finish, the destination queue drains, the Indexer finishes, and the event queue drains. The Publisher detects that its ledger is empty and no more events are arriving, declares the run complete, and `waitForCompletion()` returns.

---

## Part 3: Cross-Cutting Concerns

### Logging

Lucille uses two loggers throughout the codebase:

**`log`** — the standard class logger, used for operational messages: startup, shutdown, errors, metrics summaries, retry warnings, and anything an operator needs to monitor a running system.

**`DocLogger`** — a dedicated logger named `com.kmwllc.lucille.core.DocLogger`, used exclusively for per-document lifecycle events. Every significant transition a document makes is logged here at INFO level: publishing, pipeline entry and exit per stage, indexer receipt, and success or failure. By routing the DocLogger to a separate file or suppressing it in production, operators can get a complete per-document audit trail without noise from operational messages.

### MDC (Mapped Diagnostic Context)

The run ID and document ID are pushed into the SLF4J MDC at key points, so every log line emitted while processing a specific document automatically includes that document's ID and run ID. This makes it possible to grep a log file for a specific document ID and see its complete history across all components and threads.

### Metrics

Each component reports Codahale metrics to a shared `MetricRegistry`: the Publisher tracks publishing rate and connector latency; the Worker tracks pipeline processing time per document; the Indexer tracks indexing rate and backend latency. These are logged at the end of each run and are also available via the `WorkerPool`'s periodic status log, which fires every `log.seconds` seconds during the run.

### Run summary

At the end of every run, the Runner logs a structured summary showing how many connectors completed, how many documents succeeded, failed, and were dropped, and the total elapsed time. A connector that failed entirely is distinguished from a connector that completed but had individual document failures. The run stops at the first connector failure; subsequent connectors are listed as skipped.

### Testing

Lucille has a dedicated `RunType.TEST` mode. In test mode, a `TestMessenger` wraps the `LocalMessenger` and records every message sent between components. After a test run, `runResult.getHistory()` returns a map from connector name to `TestMessenger`, and test code can assert against the complete message history. The search backend is bypassed entirely in test mode.

---

# Appendix: Component Deep Dives

## A. The Publisher and its Accounting System

The Publisher's central data structure is `docIdsToTrack`, a synchronized `Bag<String>` keyed on user-visible document IDs. A `Bag` rather than a `Set` is used because two documents with the same ID can be published in the same run — the Bag counts duplicates, so two publishes of the same ID require two separate terminal events to clear.

**Registration ordering.** The document ID is added to `docIdsToTrack` *before* the document is placed on the source queue. This is not an accident. As soon as the document is on the queue, a Worker could process it and the Indexer could send a FINISH event. If the Publisher hadn't registered the ID yet, it would misclassify the event as "early."

**Run ID stamping.** `sendForProcessing()` calls `document.initializeRunId(runId)`, which writes the run ID into the document's `run_id` field. This field is immutable once set — calling `initializeRunId` a second time throws.

**Backpressure.** If `publisher.maxPendingDocs` is configured, `publish()` blocks when `docIdsToTrack.size() >= maxPendingDocs`. This prevents a fast Connector from flooding the system. The Publisher wakes up when `handleEvent()` removes an ID from the ledger and signals the condition.

**Collapsing mode.** Some Connectors emit multiple consecutive Documents with the same user-visible ID (e.g. a CDC stream with multiple updates to the same record). When `requiresCollapsingPublisher()` returns true, `publishInternal()` merges consecutive same-ID documents into one before calling `sendForProcessing()`. `numReceived` counts every `publish()` call; `numPublished` counts only the documents actually sent downstream.

**Out-of-order events.** Child documents (generated inside the pipeline) can complete before the Publisher receives their CREATE event, because the Worker sends CREATE and the Indexer sends FINISH asynchronously. The Publisher handles this with a second bag, `docIdsIndexedBeforeTracking`. If a terminal event arrives for an ID not yet in `docIdsToTrack`, the ID goes into this bag. When the CREATE event subsequently arrives, the Publisher checks this bag first and skips adding the ID to `docIdsToTrack` if it's already been completed.

**Run completion detection.** `waitForCompletion()` declares the run complete only when all three of these are simultaneously true: the Connector thread has terminated, `docIdsToTrack` is empty, and the last `pollEvent()` returned null. All three must hold together — any one alone is insufficient.

**Pause and resume.** The Publisher supports `pause()` and `resume()` for use cases where document publication needs to be temporarily halted. `publish()` blocks on a `Condition` when paused and wakes when `resume()` signals it.

---

## B. The Worker and Pipeline Execution

**Per-thread Pipeline isolation.** Each Worker thread constructs its own `Pipeline` instance, which constructs its own instance of every Stage. This means stages can hold stateful resources — open database connections, loaded ML models, compiled regex patterns — initialized once in `start()` and reused across all documents that thread processes, with no synchronization required. The memory cost of N model instances is the price of N-way parallelism without locks.

**Stage lifecycle.** Each Stage has three lifecycle methods: `start()` (called once when the Worker thread starts, for resource initialization), `processDocument(doc)` (called once per document), and `stop()` (called when the Worker thread shuts down, for cleanup). A Stage that fails to initialize in `start()` will cause the Worker to abort startup.

**Conditional execution.** Every Stage can declare a `conditions` block in its configuration. The base `Stage` class evaluates these conditions in `processConditional()` before calling `processDocument()`. If conditions don't match, the stage is skipped entirely for that document. Conditions can check for field presence, field value, or combinations with `all`/`any` policy. This is evaluated in the base class, so stage authors never need to implement it.

**Child documents.** `processDocument()` returns an `Iterator<Document>`. Returning `null` means no children. Returning an iterator means the stage generated child documents that should flow through downstream stages and be indexed as independent records. The Pipeline chains these iterators so the Worker sees a flat sequence. Children are always emitted before the parent — this ensures the Publisher learns about children (via CREATE events) before it learns the parent is complete (via FINISH).

**Skipped documents.** A document with `isSkipped() == true` is ignored by all downstream stages but still reaches the Indexer. This is the deletion use case: a document representing a delete operation should skip enrichment stages but still reach the Indexer to issue the delete against the backend.

**Dropped documents.** A document with `isDropped() == true` is not sent to the Indexer at all. The Worker sends a DROP event to the Publisher and discards the document. Use this when a document should be filtered out entirely.

**Error handling.** If `processDocument()` throws, the Worker catches it, logs it, sends a FAIL event for the document, and continues with the next document. The run does not stop. If a document repeatedly causes the Worker process itself to crash (a "poison pill"), the retry counter (backed by ZooKeeper or Redis, configured via `worker.maxRetries`) detects this across all Worker instances and routes the document to a dead letter queue.

---

## C. The Indexer and Batch Sending

**Batching.** Documents accumulate in a `Batch` object. The batch flushes when it reaches `indexer.batchSize` documents (default 100) or when `indexer.batchTimeout` milliseconds have elapsed since the last add or flush (default 100ms). The timeout flush is essential for low-volume scenarios — without it, a small number of documents could sit in the batch indefinitely.

**`MultiBatch` for index routing.** When `indexer.indexOverrideField` is configured, the Indexer uses a `MultiBatch` instead of a `SingleBatch`. `MultiBatch` maintains a separate `SingleBatch` per destination index, so documents going to different indices are batched independently and flushed independently.

**Batch-level vs. per-document failures.** The bulk API can fail in two ways. A batch-level failure (the entire request throws an exception) causes all documents in the batch to receive FAIL events. A per-document failure (the bulk response contains errors for specific items) causes only those documents to receive FAIL events; the rest receive FINISH events. This distinction matters for retry logic.

**Retries.** When `indexer.maxRetries > 0`, failed batch calls are retried using exponential backoff (initial wait configured via `indexer.retryWaitDurationMs`, default 1000ms). Only failures with a status code in `indexer.retryableStatusCodes` (default `[429, 503]`) trigger a retry. Other failures fail immediately. Retries are disabled by default — set `maxRetries` to a positive integer to opt in.

**Event sending.** After a batch is processed, the Indexer sends a FINISH or FAIL event for each document. The `batchComplete()` call in the `finally` block always fires regardless of outcome, so the Indexer's messenger can commit Kafka offsets even when a batch fails.

**Field filtering.** Before sending a document to the backend, `getIndexerDoc()` applies the configured whitelist/blacklist to produce the map of fields to index. Reserved fields (`___dropped`, `___skipped`, `___children`) are stripped at this point and never reach the search backend.

---

## D. Deployment Modes

Lucille supports four execution modes, selected via command-line flag:

**LOCAL** — all components run as threads in a single JVM; all inter-component communication uses in-memory queues. This is the default and is appropriate for development and single-machine production runs.

**TEST** — identical to LOCAL but the search backend is bypassed and all message traffic is recorded for inspection by test code.

**KAFKA\_LOCAL** — all components run as threads in a single JVM but communicate via Kafka. Useful for testing the Kafka integration without deploying separate processes.

**KAFKA\_DISTRIBUTED** — Workers and Indexers are assumed to be running as separate processes. The Runner only launches Connectors and waits for completion via Kafka events. This is the production distributed deployment model.

The same pipeline configuration and stage code runs in all four modes. Switching modes requires only a command-line flag change, not a code change.
