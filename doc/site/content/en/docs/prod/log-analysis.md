---
title: "Log Inspection and Analysis"
weight: 3
date: 2025-06-09
description: >
  How to read and interpret Lucille log output for monitoring, troubleshooting, and debugging.
---

## Log Format

Lucille uses SLF4J with Log4j2 as the logging backend. The log format depends on your Log4j2 configuration:

**Plain text (typical console/file output):**
```
2026-05-06 20:51:53.581 INFO  [main] c.k.l.c.Runner - Pipeline Configuration is valid.
```

**JSON (structured logging for log aggregation):**
```json
{"@timestamp":"2026-05-06T20:51:53.581Z","log.level":"INFO","message":"Pipeline Configuration is valid.","process.thread.name":"main","log.logger":"com.kmwllc.lucille.core.Runner","run_id":"c1d9413a-..."}
```

The JSON format includes the `run_id` and `id` (document ID) fields from the MDC, making it possible to filter logs by run or by document in log aggregation tools. In plain text mode, only the `message` field is typically visible.

## Anatomy of a Log Line

Every log line contains:
- **Timestamp** — when the event occurred
- **Level** — INFO, WARN, ERROR, DEBUG
- **Thread name** — identifies which component generated the message
- **Logger** — the Java class that emitted the message
- **Message** — the human-readable content
- **MDC fields** (in JSON mode) — `run_id` and optionally `id` (document ID)

---

## Identifying Components by Thread Name

The thread name tells you which component generated the message:

| Thread Name Pattern | Component |
|---|---|
| `main` | Runner (validation, connector orchestration, Publisher waitForCompletion) |
| `Lucille-{runId}-Connector` | ConnectorThread (executing the connector) |
| `Lucille-{runId}-Worker-1`, `Worker-2`, etc. | Worker threads (pipeline processing) |
| `Lucille-{runId}-Indexer` | Indexer thread |
| `Lucille-{runId}-WorkerWatcherExecutorService` | WorkerPool watcher (periodic stats, stuck-worker detection) |
| `Lucille-Worker-1` (no runId) | Worker in distributed mode (standalone process, no local run) |
| `Lucille-Indexer` (no runId) | Indexer in distributed mode (standalone process) |

### Why the run ID is in the thread name

In a typical batch ingest in single-JVM mode, all threads are dedicated to the same run, so the run ID in the thread name might seem redundant. It exists for a specific scenario: **concurrent runs in the same JVM via the REST API**.

The `lucille-api` plugin provides a REST API (built on Dropwizard) that allows runs to be triggered via HTTP. The `RunnerManager` singleton manages these runs and launches each one asynchronously via `CompletableFuture.runAsync()`. Multiple runs can execute concurrently in the same JVM — each with its own Connector, WorkerPool, Indexer, and Publisher. Without the run ID in thread names, it would be impossible to tell which log messages belong to which run when two runs are in progress simultaneously.

The thread name is constructed by `ThreadNameUtils.createName(name, runId)`:
- With a run ID: `Lucille-c1d9413a-8191-4f4a-92bb-0fc42b5499e3-Worker-1`
- Without a run ID: `Lucille-Worker-1`

### Run ID handling in distributed mode

In distributed mode, Workers and Indexers are standalone processes that stay alive indefinitely, processing documents from multiple runs over their lifetime. They are NOT started by the Runner and do NOT have a `localRunId`. This creates a distinction between two kinds of run ID:

**Thread-level run ID (in the thread name and MDC):** When a Worker or Indexer starts as a standalone process, it has no run ID — the thread name is `Lucille-Worker-1` (no UUID) and the MDC `run_id` is initially null (Worker) or "UNKNOWN" (Indexer).

**Document-level run ID (stamped on each document):** Each document carries a `run_id` field stamped by the Publisher when it was published. Documents from different runs may be interleaved on the same Kafka topic.

The Worker handles this by **updating the MDC dynamically** as it processes each document:

```java
// In Worker.run():
MDC.put(RUNID_FIELD, localRunId);  // null in distributed mode

while (running) {
    doc = messenger.pollDocToProcess();
    // If we don't have a localRunId, use the document's run_id for logging
    if (localRunId == null && doc != null) {
        MDC.put(RUNID_FIELD, doc.getRunId());
    }
    // ... process document ...
}
```

This means in distributed mode, the `run_id` in log messages changes with each document — it reflects which run produced that document, not a fixed property of the Worker process.

The Indexer uses a **stack-based MDC** (`pushByKey`/`popByKey`) because it processes batches that may contain documents from different runs:

```java
// In Indexer, when sending events per document:
if (d.getRunId() != null) {
    MDC.pushByKey(RUNID_FIELD, d.getRunId());  // temporarily set for this doc's log lines
}
messenger.sendEvent(d, "SUCCEEDED", Event.Type.FINISH);
if (d.getRunId() != null) {
    MDC.popByKey(RUNID_FIELD);  // restore previous value
}
```

**Practical implication for log analysis:** In distributed mode, you cannot rely on the thread name to identify which run a log message belongs to. Instead, filter by the `run_id` MDC field (available in JSON-formatted logs). In single-JVM mode, the thread name contains the run ID and is sufficient.

## Identifying Components by Logger

The `log.logger` field identifies the class:

| Logger | Component |
|---|---|
| `com.kmwllc.lucille.core.Runner` | Runner orchestration |
| `com.kmwllc.lucille.core.PublisherImpl` | Publisher accounting |
| `com.kmwllc.lucille.core.WorkerPool` | Worker pool management and periodic stats |
| `com.kmwllc.lucille.core.Indexer` | Indexer batching and backend communication |
| `com.kmwllc.lucille.core.Stage` | Per-stage metrics (logged at end of run) |
| `com.kmwllc.lucille.core.DocLogger` | Per-document lifecycle events (if enabled) |
| `com.kmwllc.lucille.connector.*` | Connector-specific messages |
| `com.kmwllc.lucille.stage.*` | Stage-specific messages (warnings, errors) |

---

## Reading a Run from Start to Finish

### Phase 1: Validation

The first messages in any run confirm configuration validity:

```
Pipeline Configuration is valid.
Connector Configuration is valid.
Indexer Configuration is valid.
Other (Publisher, Runner, etc.) Configuration is valid.
Starting run with id c1d9413a-8191-4f4a-92bb-0fc42b5499e3
```

If validation fails, you'll see error messages instead, and the run will not proceed. Look for messages from `com.kmwllc.lucille.core.Runner` at ERROR level.

### Phase 2: Connector Execution

For each connector, you'll see:

```
Running connector {name} feeding to pipeline {pipeline}
Starting {N} worker threads for pipeline {pipeline}
```

If a connector has no pipeline (e.g., it performs setup work only), you'll see `feeding to pipeline NOT CONFIGURED` or `null`.

### Phase 3: Periodic Progress (During Processing)

Three types of periodic messages appear every `log.seconds` (default 30 seconds):

**Publisher (main thread):**
```
{N} docs published. One minute rate: {X} docs/sec. Mean connector latency: {Y} ms/doc. Waiting on {Z} docs.
```

**WorkerPool watcher:**
```
{N} docs processed. One minute rate: {X} docs/sec. Mean pipeline latency: {Y} ms/doc.
```

**Indexer:**
```
{N} docs indexed. One minute rate: {X} docs/sec. Mean backend latency: {Y} ms/doc.
```

### Phase 4: Connector Completion

When the connector finishes publishing:
```
Connector complete. Waiting on {N} docs.
```

This means the connector thread has finished, but documents are still being processed/indexed. The system is draining.

### Phase 5: Publisher Completion

When all documents reach a terminal state:
```
Publisher complete. Mean publishing rate: {X} docs/sec. Mean connector latency: {Y} ms/doc.
{N} docs published. {C} children created. {S} success events. {F} failure events. {D} drop events.
All documents SUCCEEDED.
```

Or if there were failures:
```
{F} documents FAILED.
```

### Phase 6: Per-Stage Metrics

After each connector completes, per-stage metrics are logged:
```
Stage {name} metrics. Docs processed: {N}. Mean latency: {X} ms/doc. Children: {C}. Errors: {E}.
```

### Phase 7: Connector Timing

```
Connector {name} feeding to pipeline {pipeline} complete. Time: {X} secs.
```

### Phase 8: Run Summary

At the very end:
```
RUN SUMMARY: ...
Run took {X} secs.
```

---

## Answering Common Questions from the Logs

### Is the config valid?

Look for the four validation messages at the start. If any say something other than "is valid," the config has errors. The error messages will describe what's wrong.

### Did the connectors and pipelines start successfully?

Look for `"Starting {N} worker threads for pipeline {name}"`. If this message appears, the WorkerPool started successfully (all Stage `start()` methods completed without error). If a stage fails to start, you'll see an ERROR before this message and the run will abort.

### How many connectors were run?

Count the `"Running connector {name} feeding to pipeline {pipeline}"` messages. If the run aborted early, later connectors won't appear.

### Which stages created the most latency?

Look at the per-stage metrics at the end of each connector's execution:
```
Stage build-geoparse-data metrics. Docs processed: 286106. Mean latency: 9.2441 ms/doc.
Stage poprank-normalization-transforms metrics. Docs processed: 286106. Mean latency: 2.3383 ms/doc.
Stage normalize-i18n-commas metrics. Docs processed: 286106. Mean latency: 0.0147 ms/doc.
```

Sort by mean latency to find bottlenecks. In this example, `build-geoparse-data` at 9.24 ms/doc dominates the pipeline.

### What errors occurred?

Search for `log.level":"ERROR"` or `log.level":"WARN"`. Common patterns:
- `"Error processing document: {id}"` — a stage threw an exception for a specific document
- `"Error sending documents to index"` — a batch failed at the indexer
- `"Worker has not polled in {N} seconds"` — a worker appears stuck
- `"Connector failed to perform pre execution actions"` — preExecute threw

### Where is latency introduced?

Three distinct latency measurements appear in the periodic logs:

**Connector latency** (Publisher message): `Mean connector latency: 0.13 ms/doc`
- Time between consecutive `publish()` calls. Measures how fast the connector produces documents from the source system.

**Pipeline latency** (WorkerPool message): `Mean pipeline latency: 11.06 ms/doc`
- Time to process one document through all stages. This is the CPU-bound enrichment work.

**Backend latency** (Indexer message): `Mean backend latency: {X} ms/doc`
- Time per document for the search engine to accept a batch (total batch time / batch size).

### What does "Waiting on {N} docs" mean?

This is the Publisher's pending count — documents that have been published but haven't yet reached a terminal state (indexed, failed, or dropped). If this number stays constant while the connector is still running, it means `maxPendingDocs` backpressure is active and the connector is blocked waiting for downstream components to catch up.

### If the run is not complete yet, what are we waiting for?

Look at the most recent Publisher message:
- `"Waiting on {N} docs"` — N documents are still in-flight (being processed or indexed)
- `"Connector complete. Waiting on {N} docs"` — the connector finished publishing, but N documents haven't completed yet

If the "Waiting on" count is not decreasing over time, something is stuck. Check for:
- Worker stuck messages (`"Worker has not polled in..."`)
- Indexer showing 0 docs indexed (backend might be unreachable)
- No new WorkerPool stats appearing (workers might have crashed)

---

## Understanding "One Minute Rate"

The "one minute rate" is an **exponentially weighted moving average (EWMA)** computed by the Codahale Metrics library. It is NOT a simple count of events in the last 60 seconds.

How it works:
- The EWMA is updated every 5 seconds with the number of events since the last update
- It applies exponential decay with a 1-minute half-life
- Recent events are weighted more heavily than older events

Implications:
- At the start of a run, the one-minute rate ramps up gradually (it takes ~60 seconds to reach steady state)
- After a burst, the rate decays gradually rather than dropping instantly
- It's a smoothed approximation of throughput, not an exact count

The rate represents the **aggregate throughput across all threads** for that component. If 4 worker threads are each processing 85 docs/sec, the one-minute rate shows ~340 docs/sec.

---

## Understanding Latency Numbers in a Multithreaded System

**Pipeline latency** (`Mean pipeline latency: 11.06 ms/doc`) is the average time a single document spends being processed through all stages. This is measured per-document, per-thread. It does NOT include time spent waiting in the queue.

With 4 worker threads and 11 ms/doc pipeline latency, the theoretical throughput is:
```
4 threads × (1000 ms / 11 ms) = ~364 docs/sec
```

This matches the observed one-minute rate of ~345 docs/sec (the difference is queue polling overhead and other bookkeeping).

**Connector latency** (`Mean connector latency: 0.13 ms/doc`) is the average time between consecutive `publish()` calls on the connector thread. Low values mean the connector is producing documents faster than the pipeline can consume them (the connector is not the bottleneck).

**Backend latency** is the average time per document for the search engine to process a batch. If this is 0.00 ms/doc and 0 docs indexed, the indexer hasn't flushed a batch yet (documents are still accumulating, or the pipeline is dropping all documents before they reach the indexer).

---

## Why Different Stages Process Different Numbers of Documents

In the per-stage metrics, you might see:
```
Stage create-grouping-key metrics. Docs processed: 286106.
Stage attach-search-radius metrics. Docs processed: 953.
Stage append-grouping-context-key metrics. Docs processed: 100097.
```

This happens because of **conditional execution**. Each stage can have a `conditions` block that determines whether it processes a given document. A stage with conditions that match only a subset of documents will show a lower count. In this example:
- `create-grouping-key` processes all 286,106 documents (no conditions, or conditions that always match)
- `attach-search-radius` processes only 953 documents (its conditions match only a small subset)
- `append-grouping-context-key` processes 100,097 documents (its conditions match about a third)

This is normal and expected. It does NOT indicate errors or skipped documents.

---

## The RUN SUMMARY

At the end of a complete run, the Runner logs a structured summary including:
- Overall status (success/failure)
- Number of connectors executed
- Per-connector results (success/failure, duration, document counts)
- Total documents published, succeeded, failed, dropped
- Total run duration

If a connector failed, the summary indicates which one and why. Subsequent connectors are listed as skipped.

---

## What Is WorkerWatcherExecutorService?

The `WorkerWatcherExecutorService` is a daemon thread started by the `WorkerPool`. It runs a scheduled task every 500ms that:

1. **Logs periodic pipeline statistics** (every `log.seconds`): docs processed, one-minute rate, mean latency
2. **Detects stuck workers**: checks each worker's last poll timestamp; if any worker hasn't polled within `worker.maxProcessingSecs`, logs an error
3. **Emits heartbeats** (if `worker.enableHeartbeat` is true): writes to the heartbeat logger for Kubernetes liveness probes

The thread name includes the run ID: `Lucille-{runId}-WorkerWatcherExecutorService`. It's a daemon thread, so it doesn't prevent JVM shutdown.

---

## Single-JVM vs. Distributed Deployment Logs

### Single-JVM (LOCAL mode)

All components log to the same output (console/file). You see interleaved messages from Runner, Publisher, Workers, Indexer, and the Watcher — all in one stream. The `run_id` is the same across all messages. Thread names distinguish components.

### Distributed Deployment

Each process has its own log output:

**Runner process** — sees: validation messages, connector start/stop, Publisher messages (docs published, waiting on N, completion), per-stage metrics, run summary. Does NOT see worker or indexer processing messages.

**Worker process(es)** — each Worker JVM sees: its own WorkerPool stats (docs processed, pipeline latency), its own WorkerWatcherExecutorService messages, any stage-level warnings/errors for documents it processes. Each Worker JVM reports statistics only for the threads in that JVM. If you have 3 Worker JVMs each with 4 threads, each JVM's logs show the throughput of its own 4 threads, not the aggregate.

**Indexer process(es)** — each Indexer JVM sees: its own indexing stats (docs indexed, backend latency), any batch errors. Each Indexer reports only its own throughput.

**To get aggregate statistics** in distributed mode, you need to sum across all Worker/Indexer logs, or use a log aggregation tool that can filter by `run_id` and aggregate metrics.

---

## Common Log Patterns and What They Mean

### "First doc published after {N} ms"

Time from when the connector started to when the first document was published. High values indicate slow connector initialization (e.g., establishing a database connection, listing files in S3).

### "0 docs indexed. One minute rate: 0.00 docs/sec."

The Indexer hasn't flushed a batch yet. This is normal early in a run (documents are still accumulating in the batch) or when all documents are being dropped by the pipeline before reaching the Indexer.

### "Connector complete. Waiting on {N} docs."

The connector finished publishing all documents. The system is now draining — processing and indexing the remaining in-flight documents. The "Waiting on" count should decrease over time until it reaches 0.

### "Publisher complete."

All documents have reached a terminal state. The run is done (for this connector).

### "All documents SUCCEEDED." / "{N} documents FAILED."

Final accounting. "SUCCEEDED" means all documents were either indexed or intentionally dropped. "FAILED" means some documents encountered errors.

### "{N} drop events"

Documents that were intentionally dropped by the pipeline (via the drop flag). Dropped documents are counted as successful completions — they reached their intended terminal state.

### WARN messages during Stage start()

Warnings emitted during stage initialization (e.g., dictionary conflicts, missing optional resources). These appear on the `main` thread because `start()` is called during WorkerPool startup. They appear once per worker thread (so 4 workers = 4 identical warnings).

### "Error processing document: {id}"

A stage threw a `StageException` for this document. The document is failed and processing continues. Look for the stack trace immediately following this message.

---

## Tips for Log Analysis

1. **Filter by thread name** to isolate one component's messages.
2. **Filter by `run_id`** (in JSON mode) to isolate one run in a system that runs multiple ingests.
3. **Watch the "Waiting on" count** — if it's not decreasing, something is stuck.
4. **Compare connector rate vs. pipeline rate** — if the connector is much faster, the pipeline is the bottleneck. If they're similar, the connector might be the bottleneck.
5. **Look at per-stage metrics** to find which stage dominates pipeline latency.
6. **Check for stages with error counts > 0** — these indicate documents that failed at specific stages.
7. **In distributed mode**, remember that each JVM's stats are local. Aggregate across JVMs for the full picture.
8. **The DocLogger** (if enabled at INFO level) provides per-document tracing — every stage entry/exit for every document. This is extremely verbose but invaluable for debugging a specific document's journey. Route it to a separate file in production.
