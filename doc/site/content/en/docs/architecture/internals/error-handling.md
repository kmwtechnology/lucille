---
title: Error Handling and Fault Tolerance
weight: 7
date: 2025-06-09
description: >
  The error handling philosophy, every failure scenario catalogued, and how fault tolerance is inherited from Kafka.
---

## Philosophy

Most search ingestion projects take an optimistic approach to error handling: ingest as much content as possible and continue past errors. A search index is often useful even if it doesn't contain 100% of the data from the source system. Source systems routinely contain messy data where some records will inevitably cause errors in processing — a malformed PDF, a record with unexpected encoding, a field that exceeds a size limit. Stopping the entire ingest because one document out of millions is problematic would be counterproductive.

At the same time, it is a waste of time and resources to continue with an ingestion process if there is a configuration or initialization error that prevents it from working properly. If the pipeline configuration references a nonexistent class, or a Stage can't connect to a required external resource during startup, or the Indexer can't reach the search backend, there is no point in processing documents — they will all fail.

Furthermore, when an ingestion process consists of a sequence of connectors, we want to proceed with the next connector only if the previous one was able to complete successfully. This is because later connectors may depend on data produced by earlier ones (e.g., parent documents must be indexed before child documents that reference them).

This leads to a two-level error handling philosophy:

**Continue past per-document errors.** If a single document fails during pipeline processing or indexing, log the failure, count it, and move on. The run continues. A Lucille run with a sequence of 5 connectors can complete and report success even if individual documents encountered errors during processing or indexing and failed to reach the search backend. The run succeeded in the sense that all connectors completed their work and all documents reached a terminal state — some of those terminal states just happen to be "failed" rather than "indexed."

**Stop immediately on structural errors.** If the configuration is invalid, if a Stage can't initialize, if the Indexer can't connect, or if a Connector throws an exception during its lifecycle — stop. Don't waste time and resources on work that cannot succeed. Abort the run and report the failure clearly.

The guiding principle: *get as much data into the search engine as possible, but stop as soon as possible if you'd only be wasting your time.*

---

## Error Scenarios

### 1. Invalid Configuration

Before any run starts, `Runner.run()` calls `runInValidationMode(config)` which validates all pipelines (every Stage's SPEC), all connectors, the indexer config, and other parent configs (publisher, worker, etc.). If any validation errors are found, the run is aborted immediately — no connectors are started, no documents are processed. A `RunResult` with `status=false` is returned. All errors are reported at once (not fail-fast on the first one).

**Severity:** Fatal to the run. Nothing executes.

---

### 2. Connector preExecute() Throws

The Runner catches the `ConnectorException`, logs it, and returns a failing `ConnectorResult` with message "preExecute failed." Neither `execute()` nor `postExecute()` are called. The run is aborted (subsequent connectors in the sequence are skipped). `close()` is still called on the connector.

**Severity:** Fatal to the connector and the run.

---

### 3. Connector execute() Throws

The connector runs inside a `ConnectorThread`. If `execute()` throws, the exception is captured on the thread (`ConnectorThread.exception`). The Publisher's `waitForCompletion()` detects that the connector thread has died with an exception and returns a failing `PublisherResult`. `postExecute()` is NOT called. The run is aborted. `close()` is still called.

**Severity:** Fatal to the connector and the run.

---

### 4. Connector postExecute() Throws

`postExecute()` is only called if both `preExecute()` and `execute()` succeeded AND all published documents completed (the Publisher reported success). If `postExecute()` throws, the Runner catches it and returns a failing `ConnectorResult` with message "postExecute failed." The run is aborted. `close()` is still called.

**Severity:** Fatal to the connector and the run. Note that all documents were already successfully processed and indexed at this point — the data is in the search engine, but the run is reported as failed.

---

### 5. Stage start() Throws

`Pipeline.startStages()` is called during `Pipeline.fromConfig()`, which is called inside the `Worker` constructor. If any Stage's `start()` throws a `StageException`, the Worker constructor throws, which propagates up to `WorkerPool.start()`. The WorkerPool catches the exception, calls `stop()` on any threads that were already started, and re-throws. The Runner catches this in `runConnectorWithComponents()` and returns a failing `ConnectorResult` with message "Error starting workers for pipeline X." The run is aborted.

**Severity:** Fatal to the run. No documents are processed.

---

### 6. Stage processDocument() Throws for a Given Document

The Worker catches the exception, logs it via the DocLogger (`"Document FAILED during pipeline processing: " + doc.getId()`), sends a FAIL event to the Publisher via `messenger.sendEvent(doc, null, Event.Type.FAIL)`, commits offsets, and continues processing the next document. The failed document is counted in the Publisher's `numFailed` tally. The run does NOT stop.

**Severity:** Per-document failure. The run continues. Other documents are unaffected.

---

### 7. Worker Process Crashes While Processing a Document

This depends on the deployment mode:

- **Local mode:** The Worker thread dies. Since the Worker is a thread in the Runner's JVM, the Runner's `waitForCompletion()` will eventually time out or detect that work is not completing (pending documents never reach a terminal state). The run fails.

- **Kafka-distributed mode:** The Worker process dies. Kafka's consumer group protocol detects the dead consumer and reassigns its partitions to another available Worker instance. The document that was being processed when the crash occurred was not committed (offset not advanced), so it will be redelivered to the new Worker. The document gets reprocessed.

**Severity:** In local mode, fatal to the run. In distributed mode, recoverable — the document is retried on another Worker.

---

### 8. Same Document Causes Workers to Crash Multiple Times (Poison Pill)

When `worker.maxRetries` is configured, a `RetryCounter` (backed by ZooKeeper or Redis) tracks how many times each document has been attempted across all Worker instances. Each time a Worker picks up a document, it calls `counter.add(doc)`. If the retry count exceeds the configured maximum, the Worker:

1. Sends the document to a "failed" / dead-letter queue via `messenger.sendFailed(doc)`
2. Sends a FAIL event to the Publisher
3. Skips processing and moves on

The document is effectively quarantined. The rest of the ingest continues unaffected.

**Severity:** Per-document failure. The poison pill is isolated. The run continues.

---

### 9. Indexer Can't Connect to the Search Backend

Before the Indexer thread starts processing, `validateConnection()` is called. If it returns `false`:

- **In the Runner (local/Kafka-local mode):** The Runner logs "Indexer could not connect", calls `indexer.closeConnection()`, and returns a failing `ConnectorResult`. The run is aborted.

- **In standalone Indexer mode (distributed):** The Indexer logs the error and calls `System.exit(1)`.

- **In WorkerIndexer mode:** The `start()` method throws an `IndexerException("Indexer could not connect")`, which prevents the WorkerIndexer from starting.

**Severity:** Fatal. No documents are indexed.

---

### 10. A Batch Fails During Indexing

There are two sub-cases:

**10a. The entire batch call throws an exception (batch-level failure):**
The Indexer catches the exception, logs it, and sends a FAIL event for every document in the batch. If retries are configured (`indexer.maxRetries > 0`) and the failure's status code is in `indexer.retryableStatusCodes` (default `[429, 503, -1]`), the batch is retried with exponential backoff before being declared failed. After all retries are exhausted (or if the status code is not retryable), all documents in the batch receive FAIL events. The Indexer continues processing subsequent batches. The `batchComplete()` call in the `finally` block always fires regardless of outcome.

**10b. The bulk API succeeds but reports per-document failures:**
`sendToIndex()` returns a set of failed document/reason pairs. The Indexer sends FAIL events for those specific documents and FINISH events for the rest. The Indexer continues normally.

**Severity:** Per-batch or per-document failure. The run continues. Failed documents are counted in the Publisher's accounting.

---

### 11. Indexer Process Crashes

- **Local mode:** The Indexer thread dies. Documents accumulate on the indexing queue but are never consumed. The Publisher's `waitForCompletion()` will eventually time out because pending documents never reach a terminal state. The run fails.

- **Kafka-distributed mode:** The Indexer process dies. Kafka's consumer group protocol reassigns its partitions to another available Indexer instance. Unacknowledged documents (those in the batch that was being processed) are redelivered to the new Indexer. Since search engine upserts are idempotent, re-indexing an already-indexed document produces the same result.

**Severity:** In local mode, fatal to the run. In distributed mode, recoverable.

---

### 12. Runner Process Crashes

The Runner has a signal handler (`Signal.handle(new Signal("INT"), ...)`) that attempts a clean shutdown: closing the Connector, closing the Publisher, stopping the WorkerPool, and terminating the Indexer. If the Runner crashes hard (e.g., OOM, kill -9):

- **Local mode:** Everything dies — all components are threads in the same JVM. The run is lost. Documents that were in-flight are lost (they were in in-memory queues).

- **Kafka-distributed mode:** Only the Connector and Publisher die. Workers and Indexers are separate processes and continue running. However, no new documents will be published, and the Publisher's accounting is lost. Documents already on Kafka topics will still be processed and indexed by the Workers and Indexers, but there is no completion detection — the run has no coordinator to declare it done.

**Severity:** Fatal to the run in all modes. In distributed mode, in-flight work is not lost (it's on Kafka), but run-level accounting is.

---

### 13. Additional Error Scenarios

**Publisher fails to send an event (messenger.sendEvent throws):**
The Worker and Indexer both catch this and log it. The consequence is severe: if a FINISH or FAIL event is lost, the Publisher will never learn that the document completed. `waitForCompletion()` will hang until timeout. The code logs `"RUN WILL HANG"` in this case. This is a rare edge case — it would require the event queue (Kafka topic or in-memory queue) to be unavailable.

**Worker watcher detects a stuck Worker:**
The `WorkerPool` runs a watcher thread that checks each Worker's last poll timestamp. If a Worker hasn't polled in `worker.maxProcessingSecs` seconds (default 10 minutes), it logs an error. If `worker.exitOnTimeout` is configured, it calls `System.exit(1)`. This handles the case where a Stage enters an infinite loop or deadlock on a particular document.

**Connector's close() throws:**
The Runner catches this and returns a failing `ConnectorResult`. Since `close()` is called in a `finally` block after all other work, the documents may have been successfully processed, but the run is reported as failed.

**Publisher's waitForCompletion() times out:**
If the configured `runner.connectorTimeout` (default 24 hours) is exceeded, `waitForCompletion()` returns a failing `PublisherResult`. The run is reported as failed. This catches scenarios where documents are stuck (lost events, hung Workers, etc.).

**Kafka offset commit fails:**
The Worker calls `messenger.commitPendingDocOffsets()` after processing each document. If this throws, the Worker logs the error and continues. The consequence is that if the Worker later crashes, the document may be redelivered and reprocessed (at-least-once semantics). This is safe because pipeline processing is expected to be idempotent.

**Document publish fails (publisher.publish() throws):**
If the Publisher's `sendForProcessing()` throws (e.g., the processing queue is broken), the exception propagates up to the Connector's `execute()` method. Unless the Connector catches it, this becomes scenario #3 (connector execute throws) and the run fails.

---

---

## Fault Tolerance

### Single-JVM Mode Is Not Fault-Tolerant

In local mode, all components — Connector, Workers, Indexer, Publisher — run as threads inside a single JVM. If the JVM crashes or the server goes down, everything is lost: in-flight documents were in in-memory queues and are gone. There is no recovery mechanism. The run must be restarted from the beginning.

This is an acceptable tradeoff for development, testing, and small production jobs where a restart is cheap. For workloads where fault tolerance matters, Lucille's distributed mode with Kafka provides the answer.

### Fault Tolerance Is Inherited from Kafka

Lucille does not implement its own fault-tolerance logic (no custom write-ahead logs, no checkpointing to disk, no replication protocol). Instead, it inherits fault tolerance from Kafka by treating Kafka topics as durable queues and relying on Kafka's consumer group protocol for work reassignment when a process dies.

The key to making this work is **proper offset management**. Lucille's contract is:

> A Worker only commits a Kafka offset once the current document has been fully processed and placed on the destination queue.

If the Worker crashes while processing a document, that Kafka message's offset has not been committed. When Kafka detects the dead consumer (via missed heartbeats), it reassigns that partition to another Worker in the consumer group. The new Worker begins consuming from the last committed offset — which is before the document that caused the crash. That document is redelivered and reprocessed.

### Two Levels of Offset Commitment

Lucille has two deployment patterns with different offset semantics:

**Fully distributed mode (separate Worker and Indexer processes):**
The Worker commits offsets via `KafkaWorkerMessenger.commitPendingDocOffsets()` after processing each document and placing it on the indexing topic. The guarantee is: if the Worker crashes, unprocessed documents are redelivered. However, documents that were processed and placed on the indexing topic but not yet indexed are safe — they're on Kafka and will be picked up by an Indexer.

**WorkerIndexer mode (paired Worker + Indexer in one process):**
The offset flow is more sophisticated. The Indexer, after successfully sending a batch to the search backend, places the offsets of the indexed documents on an in-memory queue. The Worker reads from this queue during its `commitPendingDocOffsets()` call and commits them back to Kafka. The guarantee is stronger: offsets are only committed after documents are *indexed*, not just processed. If the WorkerIndexer crashes, documents that were processed but not yet indexed are redelivered and re-indexed. Since search engine upserts are idempotent, this produces correct results.

### What Fault Tolerance Guarantees

- **No data loss.** A document that enters the system (is placed on a Kafka topic) will eventually be processed and indexed, or will exhaust its retry count and be routed to a dead-letter queue. It will not silently disappear.
- **At-least-once processing.** A document may be processed more than once if a crash occurs after processing but before offset commit. Pipeline stages and indexing operations should be idempotent (search engine upserts naturally are).
- **Automatic recovery.** No manual intervention is required when a Worker or Indexer crashes. Kafka's consumer group protocol handles partition reassignment automatically. New instances can be added at any time.

### What Fault Tolerance Does Not Guarantee

- **Exactly-once processing.** There is a window between completing work and committing the offset where a crash causes redelivery. This is the standard Kafka at-least-once pattern.
- **Run-level accounting survives a Runner crash.** The Publisher (which tracks completion) runs alongside the Connector in the Runner process. If the Runner dies, accounting is lost. Documents on Kafka will still be processed, but no component knows when "the run" is done.
- **Order preservation after redelivery.** A redelivered document may be processed after documents that were originally behind it in the queue. For most search ingestion workloads this is acceptable — the final state of the index is the same regardless of processing order.

---

## Summary Table

| Scenario | Scope | Run Continues? | Data Lost? |
|---|---|---|---|
| Invalid config | Run | No (never starts) | N/A |
| preExecute throws | Connector/Run | No | N/A |
| execute throws | Connector/Run | No | Unpublished docs never enter system |
| postExecute throws | Connector/Run | No | No (docs already indexed) |
| Stage start() throws | Run | No (never starts) | N/A |
| processDocument throws | Document | Yes | That document fails |
| Worker crash (local) | Run | No | In-flight docs lost |
| Worker crash (distributed) | Document | Yes (redelivered) | No |
| Poison pill | Document | Yes (quarantined) | That document fails |
| Indexer can't connect | Run | No (never starts) | N/A |
| Batch fails (batch-level) | Batch | Yes | Those docs fail |
| Batch fails (per-doc) | Document | Yes | Those docs fail |
| Indexer crash (local) | Run | Eventually times out | In-flight batch lost |
| Indexer crash (distributed) | Batch | Yes (redelivered) | No |
| Runner crash (local) | Run | No | Everything in-flight lost |
| Runner crash (distributed) | Run | Partially | Accounting lost, data on Kafka survives |
| Event send fails | Run | Hangs until timeout | No data lost, but run never completes |
| Worker stuck | Run | Depends on config | Depends on exitOnTimeout |
