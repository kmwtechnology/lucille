---
title: "Document Lifecycle"
weight: 6
date: 2025-06-09
description: >
  The complete journey of a single Document through Lucille, from raw data in a source system to a searchable record in the search backend.
---

This page traces a single Document from the moment it exists only as raw data in a source system to the moment it is retrievable by a search query. It takes the Lucille architecture as a given — for the conceptual explanation of how the system is structured, see [Parallelizing Search ETL]({{< relref "docs/architecture/overview/parallelizing-search-etl" >}}) and [Pluggable Queueing]({{< relref "docs/architecture/overview/pluggable-queueing" >}}).

---

## The Happy Path

Before a document's lifecycle begins, the system components must be set up. A batch ingest is launched via the Runner, which first validates the configuration. Assuming a single-connector config, the Runner creates a Publisher, passes it to the Connector, and launches the Connector in its own thread. The Runner then calls `publisher.waitForCompletion()` and blocks. In distributed mode, the Worker and Indexer processes would have been started separately beforehand. In local mode, the Runner starts the Worker and Indexer as threads in the same JVM. With this infrastructure in place, documents can begin their journey.

### Step 1 — Raw data in the source system

The document begins as a row in a database table, a file on a filesystem, a message on a Kafka topic, or any other source that Lucille's Connector knows how to read. At this point it is not yet a Lucille Document — it is raw data in whatever format the source system uses.

### Step 2 — The Connector reads the record and creates a Document

Inside `connector.execute(publisher)`, the Connector reads the next record from its source and constructs a `Document` with a stable, user-defined ID. The ID must be deterministic — if the pipeline is re-run, the same source record must produce the same document ID so that the Indexer's upsert semantics correctly update rather than duplicate the record in the search backend.

The Connector calls `publisher.publish(document)`.

### Step 3 — The Publisher registers the Document and puts it on the processing queue

Before placing the document on the processing queue, the Publisher does two things in order:

1. Stamps the document with the `run_id` — an immutable field set once and used for log correlation, event routing, and Kafka topic naming throughout the document's lifetime.
2. Registers the document's ID in its accounting ledger (`docIdsToTrack`). Registration happens *before* the document is placed on the queue — the moment it is on the queue, a Worker could process it and the Indexer could send a FINISH event. If the Publisher hadn't registered the ID yet, it would misclassify that event.

The document is then placed on the **processing queue**. In distributed mode, this crosses a Kafka boundary: the document becomes a message on the `{pipeline}_source` Kafka topic, serialized and persisted.

No lifecycle event is sent at this point — the Publisher tracks the document internally through the accounting ledger rather than via the event queue.

### Step 4 — A Worker thread picks up the Document

A Worker thread is blocking on `processingQueue.poll()`. In distributed mode, this is a Kafka consumer `poll()` call — each Worker thread is an independent Kafka consumer within the consumer group, and Kafka's consumer group protocol has assigned it one or more partitions of the source topic. The document is delivered to exactly one Worker thread.

The Worker passes the document through each Stage in the pipeline in sequence.

### Step 5 — Pipeline processing

Each Stage receives the document, performs its transformation — extracting text, running NLP, generating an embedding, querying a database — and returns the result.

### Step 6 — The parent Document moves to the indexing queue

Once the parent document has passed through all pipeline stages, the Worker places it on the **indexing queue**. In distributed mode, this crosses a second Kafka boundary: the document becomes a message on the `{pipeline}_dest` Kafka topic.

### Step 7 — The Indexer batches and sends the Document

The Indexer thread is blocking on `indexingQueue.poll()`. It accumulates documents in a batch. When the batch reaches its configured size (`indexer.batchSize`, default 100 documents) or its timeout elapses (`indexer.batchTimeout`, default 100ms), the Indexer issues a single bulk API call to the search backend.

Before sending, the Indexer strips reserved internal fields (`___dropped`, `___skipped`, `___children`) and applies any configured field whitelist or blacklist. The document that reaches the search backend contains only the fields you want indexed.

The search backend accepts the document. The document now exists in the index.

### Step 8 — The Indexer sends a FINISH event

After a successful bulk operation, the Indexer sends a **`FINISH` event** to the event queue for each document in the batch — including our document. In distributed mode this event travels on the `{pipeline}_event_{runId}` Kafka topic, which is unique per run so that events from concurrent runs never interfere with each other.

The Publisher's polling loop receives the FINISH event, looks up the document's ID in `docIdsToTrack`, and removes it. The document's lifecycle is complete.

### Step 9 — The Document is searchable

The document has been accepted by the search backend. Once the backend makes it visible — via a scheduled commit in Solr, or after the refresh interval in Elasticsearch and OpenSearch — a query can retrieve it. Lucille does not issue commits; visibility timing is controlled entirely by the search backend's configuration. The enrichment performed during pipeline processing — the extracted text, the entity tags, the vector embedding — is available for full-text search, faceting, and semantic retrieval.

---

After all documents (parents and children) have completed their individual lifecycles, the framework determines that the run is complete. The Publisher continuously evaluates three conditions: (1) the Connector thread has terminated (no more documents will be published), (2) the event queue is empty (no more events are in transit), and (3) `docIdsToTrack` is empty (every document has reached a terminal state). All three must hold simultaneously. When they do, `waitForCompletion()` returns and the Runner logs the run summary.

---

## The Detailed Path

The Happy Path above omits edge cases, error handling, logging, metrics, backpressure, routing, and child documents. This section walks through the same journey again, annotating each step with everything that actually happens. A developer debugging "why didn't my document get indexed?" can walk through these steps to identify where the document's journey diverged.

### Step 1 — Raw data in the source system

Same as the Happy Path. No additional mechanisms apply until the Connector reads the record.

### Step 2 — The Connector reads the record and creates a Document

Everything from the Happy Path, plus:

- **Backpressure (maxPendingDocs).** If `publisher.maxPendingDocs` is configured and the pending count has reached the threshold, `publisher.publish()` blocks here — the Connector thread waits until downstream components process enough documents to bring the count below the max.
- **Backpressure (queue capacity).** In local mode, if `publisher.queueCapacity` is reached, the `put()` call blocks until a Worker consumes a document from the queue.
- **Pause/resume.** If `publisher.pause()` has been called (rare, used in specialized deployment patterns), `publish()` blocks until `resume()` is called.
- **Metrics.** The Publisher's timer measures the gap between consecutive `publish()` calls — this becomes the "Mean connector latency" metric visible in periodic logs.
- **Logging.** The DocLogger logs: `"Publishing document {id}."`

### Step 3 — The Publisher registers the Document and puts it on the processing queue

Everything from the Happy Path, plus:

- **Collapsing mode.** If collapsing mode is active and the previous document had the same ID, the Publisher merges this document's fields into the previous one via `setOrAddAll()` and does NOT place it on the queue yet. It waits for a document with a different ID before sending the merged result.
- **MDC.** The document's `run_id` field is now set and immutable. All subsequent log lines for this document (via MDC) will include this run_id.
- **Serialization.** In local mode, the Document object (a Jackson `ObjectNode` wrapper) is placed directly on the in-memory queue — no serialization occurs. In distributed mode, the Document is serialized to a JSON string and produced to the Kafka source topic. The document's `id` becomes the Kafka message key (ensuring ordering guarantees), and the full JSON representation becomes the message value. The JSON format means documents on Kafka are human-readable with standard tooling.

### Step 4 — A Worker thread picks up the Document

Everything from the Happy Path, plus:

- **Deserialization.** In distributed mode, the Worker's Kafka consumer receives the JSON message and deserializes it back into a `KafkaDocument` (a `Document` subclass that also carries the Kafka partition, offset, and key metadata). In local mode, the Worker receives the original `Document` object directly from the in-memory queue — no deserialization needed.
- **Stuck-worker detection.** The Worker updates its `pollInstant` timestamp — the WorkerPool watcher uses this to detect stuck workers. If the Worker doesn't poll again within `worker.maxProcessingSecs`, the watcher logs an error (and optionally exits the JVM if `worker.exitOnTimeout` is true).
- **MDC in distributed mode.** The Worker updates the MDC `run_id` from the document's stamped run_id (since the Worker thread may process documents from different runs over its lifetime).
- **Poison pill detection.** If `worker.maxRetries` is configured, the Worker checks the RetryCounter. If this document has exceeded its retry limit (because it crashed Workers on previous attempts), the Worker sends it to the dead letter queue and sends a FAIL event — it never enters the pipeline.

### Step 5 — Pipeline processing

Everything from the Happy Path, plus:

- **Conditional execution.** Before each Stage, the framework evaluates the `conditions` block. If conditions don't match, the Stage is skipped entirely for this document — `processDocument()` is never called, no time is recorded for that Stage, and the DocLogger logs `"Stage {name} did not process {id}."`
- **Dropped/skipped bypass.** If the document is marked as dropped (`___dropped = true`) or skipped (`___skipped = true`), ALL remaining stages are bypassed.
- **Per-stage metrics.** For each Stage that does execute, the framework records processing time in a per-stage Timer. This becomes the "Mean latency: X ms/doc" in the per-stage metrics logged at end of run. The stage's child Counter is incremented for each child emitted.
- **Per-stage logging.** The DocLogger logs `"Stage {name} to process {id}"` before and `"Stage {name} done processing {id}"` after each stage execution.
- **Error handling.** If a Stage throws `StageException`, the Worker catches it, sends a FAIL event, and the document's lifecycle ends here — it never reaches the Indexer. The stage's error Counter is incremented.
- **Child document emission.** A Stage can emit child documents by returning them from `processDocument()` as an `Iterator<Document>`. The most common use case is chunking: a `ChunkText` Stage attaches text chunks to the parent, and a subsequent `EmitNestedChildren` Stage converts them into independently flowing documents. Each emitted child becomes a separate search record. When the Worker emits a child, it immediately sends a **`CREATE` event** to the event queue — before the parent document's pipeline execution continues. This ordering guarantee ensures the Publisher registers the child's ID in `docIdsToTrack` before it could possibly receive a completion event for the parent (which would otherwise make the run appear done while children are still in flight). The child joins the processing queue and is picked up by a Worker thread — possibly the same one, in a later poll iteration — for its own independent pipeline run.

### Step 6 — The parent Document moves to the indexing queue

Everything from the Happy Path, plus:

- **Dropped documents.** If the document is dropped (`___dropped = true`), it does NOT go to the indexing queue. The Worker sends a DROP event to the Publisher and discards it. The document's lifecycle ends here.
- **Skipped documents.** If the document is skipped (`___skipped = true`), it DOES go to the indexing queue — it bypassed enrichment stages but still needs to reach the Indexer (typically to issue a delete against the search backend).
- **Metrics.** The Worker's processing Timer stops — the elapsed time contributes to "Mean pipeline latency" in periodic logs.
- **Offset management.** In distributed mode, the Worker commits the Kafka offset for this document. In WorkerIndexer mode, the commit is deferred until after indexing succeeds.
- **Serialization to indexing queue.** In distributed mode, the document is serialized to JSON again and produced to the Kafka dest topic (again with the document ID as the message key). In local mode or WorkerIndexer mode, the Document object is placed directly on an in-memory queue.

### Step 7 — The Indexer batches and sends the Document

Everything from the Happy Path, plus:

- **Index routing.** If `indexer.indexOverrideField` is configured and the document has that field, the document is routed to a different index/collection than the default. With MultiBatch, it goes into a separate batch for that destination.
- **Deletion handling.** If the document is marked for deletion (via `deletionMarkerField` + `deletionMarkerFieldValue`), the Indexer issues a delete operation instead of an index operation. If `deleteByFieldField` and `deleteByFieldValue` are also present, it's a delete-by-query.
- **ID override.** If `indexer.idOverrideField` is configured, the document's ID in the search backend is taken from that field rather than the internal document ID.
- **Field filtering.** The configured whitelist/blacklist is applied — only the desired fields reach the search backend. Reserved fields (`___dropped`, `___skipped`, `___children`) are always stripped.
- **Retries.** If the bulk API call fails with a retryable status code (429, 503) and `indexer.maxRetries > 0`, the entire batch is retried with exponential backoff. The document may be sent multiple times before succeeding or being declared failed.
- **Metrics.** The Indexer's Meter and Histogram record the batch: docs/sec throughput and per-doc backend latency.

### Step 8 — The Indexer sends a FINISH event

Everything from the Happy Path, plus:

- **Per-document failures.** If the bulk API reported a per-document failure for this specific document (e.g., schema violation), a FAIL event is sent instead of FINISH. The document was processed successfully through the pipeline but rejected by the search backend.
- **Offset commitment.** The `batchComplete()` call fires in the `finally` block regardless of success or failure — this allows Kafka offset commits in WorkerIndexer mode even when a batch fails.
- **Backpressure release.** The Publisher receives the event, removes the document's ID from `docIdsToTrack`, and decrements the pending count. If `maxPendingDocs` was blocking the Connector, this may unblock it.
- **Run accounting.** The document contributes to the run's `numSucceeded` (FINISH), `numFailed` (FAIL), or `numDropped` (DROP) count, which appears in the final run summary.

### Step 9 — The Document is searchable

Everything from the Happy Path, plus:

- **Run summary.** The run summary logged at completion includes this document in its counts: e.g., `"200000 docs succeeded. 0 docs failed. 0 docs dropped."`
- **Per-stage metrics.** The per-stage metrics logged at completion show how much time this document (aggregated with all others) spent in each stage.
- **Audit trail.** If the DocLogger was enabled at INFO level, a complete audit trail exists for this document — every stage entry/exit, every queue transition, the FINISH event — filterable by document ID via MDC.

---

For detailed explanations of how each component works internally, see the [Internals](/docs/architecture/internals/) section — particularly [Publisher Accounting](/docs/architecture/internals/publisher-accounting/), [Pipeline Internals](/docs/architecture/internals/pipeline-internals/), [Kafka Integration](/docs/architecture/internals/kafka-integration/), and [Metrics and Observability](/docs/architecture/internals/metrics-observability/).
