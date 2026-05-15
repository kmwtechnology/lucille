---
title: "Performance Tuning"
weight: 7
date: 2025-06-09
description: >
  How to identify bottlenecks, interpret throughput metrics, and tune Lucille for maximum ingestion performance.
---

## Before You Optimize: Choosing the Right Deployment Mode

Distributed mode provides a pathway to higher throughput by running multiple Worker and Indexer processes across machines. However, there is inherent overhead in serializing and deserializing documents across Kafka and process boundaries — every document is converted to JSON, written to Kafka, read from Kafka, and parsed back into a Document object at each hop.

**Distributed mode is not necessary or justified for all projects.** For a small ingestion task (thousands to low millions of documents with lightweight enrichment), local single-JVM mode is often sufficient and faster end-to-end than distributed mode because it avoids all serialization overhead. The in-memory queues in local mode have near-zero latency compared to Kafka round-trips.

Consider distributed mode when:
- Pipeline processing is CPU-bound and a single machine's cores are saturated
- The ingest volume is large enough that the serialization overhead is amortized across many documents
- You need fault tolerance (crash recovery via Kafka redelivery)
- You need to scale Workers independently of the Connector or Indexer

Consider local mode when:
- The ingest completes in a reasonable time on a single machine
- The pipeline is I/O-bound (calling external APIs) rather than CPU-bound — adding threads in local mode is simpler than adding processes
- The operational complexity of Kafka is not justified for the workload
- You're in development or testing

You can always start in local mode and move to distributed later without changing pipeline code — the transition is a command-line flag, not a rewrite.

---

## Reducing Document Size

Documents flow through queues, are serialized to Kafka (in distributed mode), and are held in memory during processing. Smaller documents mean less memory consumption, less data transfer, and faster serialization. This is one of the highest-leverage optimizations available.

### Remove Unnecessary Fields Early

If a field is not needed by downstream stages or the search backend, remove it as early as possible:

**Best: Don't populate it in the Connector.** If the Connector reads from a database, only SELECT the columns you need. If it reads files, don't load `file_content` unless a downstream stage requires it.

**Second best: Remove it at the beginning of the pipeline.** Use `DeleteFields` as one of the first stages:

```hocon
stages: [
  {
    class: "com.kmwllc.lucille.stage.DeleteFields"
    fields: ["raw_html", "debug_info", "internal_metadata"]
  },
  // ... remaining stages that don't need those fields
]
```

**After extraction: Remove large intermediate fields.** If a stage extracts text from `file_content`, delete `file_content` immediately after — it's no longer needed and may be megabytes per document:

```hocon
stages: [
  {
    class: "com.kmwllc.lucille.tika.stage.TextExtractor"
    byteArrayField: "file_content"
    dest: "body"
  },
  {
    class: "com.kmwllc.lucille.stage.DeleteFields"
    fields: ["file_content"]
  },
  // ... remaining stages operate on "body", not the raw bytes
]
```

### Consider Whether Data Needs to Live on the Document

Not all data needs to be carried on the Document itself. Consider whether a **reference** (file path, URL, database key) is sufficient instead of the actual content:

- If a stage needs to read a file, the Connector can store the file path on the document and the stage can read the file directly — rather than the Connector loading the entire file into a `byte[]` field.
- If a stage needs to query an API, it can use an ID or URL stored on the document to make the call at processing time — rather than the Connector pre-fetching all API responses.
- If the search backend doesn't need the raw content (only extracted/enriched fields), don't carry the raw content through the entire pipeline.

This is especially important in distributed mode where every byte on the document is serialized to Kafka at each hop. A 10MB PDF binary on a document means 10MB written to the source topic, 10MB read by the Worker, 10MB written to the dest topic, and 10MB read by the Indexer — 40MB of I/O for content that might only be needed by one stage.

---

## Using Conditional Execution to Avoid Unnecessary Work

Every Stage supports a `conditions` block that controls whether it executes for a given document. Use this to ensure expensive stages are not applied to documents that don't need them:

```hocon
stages: [
  {
    class: "com.kmwllc.lucille.tika.stage.TextExtractor"
    byteArrayField: "file_content"
    conditions: [
      { fields: ["file_content"], operator: "must" }
    ]
  },
  {
    class: "com.kmwllc.lucille.stage.OpenAIEmbed"
    source: "body"
    conditions: [
      { fields: ["body"], operator: "must" }
      { fields: ["content_type"], values: ["article", "page"], operator: "must" }
    ]
    conditionPolicy: "all"
  }
]
```

In this example:
- `TextExtractor` only runs on documents that actually have `file_content` — documents without it skip the stage entirely (zero cost).
- `OpenAIEmbed` only runs on documents that have a `body` field AND a `content_type` of "article" or "page" — other documents skip the expensive embedding call.

**This is free performance.** Conditional execution is evaluated in the base `Stage` class before `processDocument()` is called. There is no overhead for documents that don't match — they are simply not processed by that stage. For pipelines that handle heterogeneous documents (some with files, some without; some needing embeddings, some not), conditions can dramatically reduce the average pipeline latency.

---

## Parallelizing Multiple Pipelines

If your ingest involves multiple pipelines (e.g., one for database records, one for files, one for an API source), consider whether they need to run sequentially or can run in parallel.

### Sequential Pipelines (Single Config)

Use a single Lucille config with multiple connector/pipeline pairs when:
- One pipeline's output is a dependency for another (e.g., parent documents must be indexed before child documents that reference them)
- You need a single run_id across all pipelines for unified accounting and log correlation
- You want a single run summary that reports success/failure across all connectors

```hocon
connectors: [
  { name: "parents", class: "...", pipeline: "parent-pipeline" },
  { name: "children", class: "...", pipeline: "child-pipeline" }
]

pipelines: [
  { name: "parent-pipeline", stages: [...] },
  { name: "child-pipeline", stages: [...] }
]
```

In this configuration, `parents` completes fully (all documents indexed) before `children` starts. All documents share a single `run_id`.

### Parallel Pipelines (Separate Configs)

If your pipelines don't depend on each other, you can run them in parallel with separate Lucille configs launched via separate Runner invocations:

```bash
#!/bin/bash
# Run three independent pipelines in parallel
java -Dconfig.file=conf/database-ingest.conf -cp 'target/lib/*' com.kmwllc.lucille.core.Runner &
java -Dconfig.file=conf/file-ingest.conf -cp 'target/lib/*' com.kmwllc.lucille.core.Runner &
java -Dconfig.file=conf/api-ingest.conf -cp 'target/lib/*' com.kmwllc.lucille.core.Runner &
wait
echo "All ingests complete"
```

Each invocation gets its own `run_id`, its own Publisher, its own Workers, and its own run summary. They execute concurrently and independently.

**Tradeoffs:**
- **Pro:** Total wall-clock time is reduced (pipelines overlap instead of running sequentially)
- **Pro:** Each pipeline can be tuned independently (different thread counts, batch sizes)
- **Pro:** A failure in one pipeline doesn't abort the others
- **Con:** Each run has a separate `run_id` — you can't correlate all documents from a single "ingest session" by run_id
- **Con:** If pipelines write to the same index, you must ensure their document IDs don't collide (use `docIdPrefix`)
- **Con:** Resource contention — multiple JVMs competing for CPU, memory, and search backend capacity

### When to Choose Each Approach

| Scenario | Approach |
|---|---|
| Pipeline B depends on Pipeline A's output | Sequential (single config) |
| Need unified run accounting across all pipelines | Sequential (single config) |
| Pipelines are independent, wall-clock time matters | Parallel (separate configs) |
| Different pipelines need different Worker thread counts | Parallel (separate configs) |
| Running on a machine with many cores and plenty of memory | Parallel (separate configs) |
| Running on a constrained machine | Sequential (single config) to avoid resource contention |

---

## Understanding Where Time Is Spent

Lucille's concurrent architecture means three things happen simultaneously: the Connector reads from the source, Workers process documents through the Pipeline, and the Indexer sends batches to the search backend. The overall throughput is limited by whichever component is slowest.

The periodic log messages tell you which component is the bottleneck:

```
INFO PublisherImpl: 80230 docs published. One minute rate: 483.11 docs/sec. Mean connector latency: 0.10 ms/doc. Waiting on 10019 docs.
INFO WorkerPool: 81269 docs processed. One minute rate: 390.46 docs/sec. Mean pipeline latency: 9.82 ms/doc.
INFO Indexer: 17016 docs indexed. One minute rate: 455.07 docs/sec. Mean backend latency: 6.90 ms/doc.
```

### Interpreting the Numbers

**Connector rate** (Publisher message): How fast the Connector produces documents. A very low connector latency (< 1 ms/doc) means the Connector is not the bottleneck — it's producing faster than downstream can consume.

**Pipeline rate** (WorkerPool message): How fast Workers process documents through all Stages. This is the aggregate rate across all Worker threads.

**Indexer rate** (Indexer message): How fast the search backend accepts batches.

**"Waiting on N docs"**: The number of documents currently in flight (published but not yet indexed or failed). If this number is consistently at `maxPendingDocs` or `queueCapacity`, the Connector is being throttled by backpressure — downstream is the bottleneck.

### Identifying the Bottleneck

| Symptom | Bottleneck | Action |
|---|---|---|
| Pipeline rate << Connector rate, "Waiting on" at max | Pipeline (CPU-bound) | Add Worker threads or Worker processes |
| Indexer rate << Pipeline rate | Search backend (I/O-bound) | Tune batch size, add Indexer processes, or scale the backend |
| Connector rate << Pipeline rate, "Waiting on" is low | Connector (source I/O) | Optimize source queries, add parallelism in Connector |
| All rates similar and low | Likely a single slow Stage | Check per-stage metrics at end of run |

---

## Per-Stage Bottleneck Analysis

At the end of each connector's execution, Lucille logs per-stage metrics:

```
Stage text-extraction metrics. Docs processed: 286106. Mean latency: 45.2 ms/doc. Children: 0. Errors: 0.
Stage entity-recognition metrics. Docs processed: 286106. Mean latency: 12.8 ms/doc. Children: 0. Errors: 0.
Stage copy-fields metrics. Docs processed: 286106. Mean latency: 0.003 ms/doc. Children: 0. Errors: 0.
```

**Sort by mean latency** to find the dominant stage. In this example, `text-extraction` at 45.2 ms/doc accounts for ~78% of the total pipeline latency. Optimizing or parallelizing this stage would have the most impact.

### What the Latency Numbers Mean in a Multithreaded System

Pipeline latency is measured **per-document, per-thread**. It does NOT include time spent waiting in the queue. With N Worker threads:

```
theoretical max throughput = N threads × (1000 ms / mean_pipeline_latency_ms)
```

Example: 4 threads × (1000 / 9.82) = ~407 docs/sec. If the observed rate is significantly lower than this theoretical max, something else is constraining throughput (queue contention, GC pauses, or the Indexer not consuming fast enough).

### Why Different Stages Process Different Document Counts

If you see:
```
Stage extract-text metrics. Docs processed: 286106.
Stage attach-radius metrics. Docs processed: 953.
```

This is **conditional execution** — the second stage has conditions that match only a subset of documents. This is normal and expected, not an error.

---

## Tuning Worker Threads

### When to Add Threads

Add Worker threads when the pipeline is CPU-bound (pipeline rate is the bottleneck) and the machine has spare CPU cores.

```hocon
worker {
  threads: 8  # default is 1
}
```

### Diminishing Returns

Each Worker thread creates its own Pipeline instance with its own Stage instances. If a Stage loads a large model (e.g., 500MB NLP model), each thread loads its own copy. With 8 threads, that's 4GB of model memory.

Monitor CPU utilization and memory. If adding threads doesn't increase throughput proportionally, you've hit either:
- Memory limits (GC pressure from too many model copies)
- I/O limits (all threads waiting on the same external service)
- Queue contention (unlikely with Lucille's design, but possible at very high thread counts)

### Per-Pipeline Thread Configuration

Threads can be configured per-pipeline rather than globally:

```hocon
pipelines: [
  {
    name: "heavy-pipeline"
    threads: 8
    stages: [...]
  },
  {
    name: "light-pipeline"
    threads: 2
    stages: [...]
  }
]
```

---

## Tuning Indexer Batching

### Batch Size

Larger batches mean fewer API calls to the search backend, which generally improves throughput:

```hocon
indexer {
  batchSize: 500  # default is 100
}
```

However, larger batches also mean:
- More memory used to hold the batch
- Higher latency before documents appear in the index (they wait longer in the batch)
- Larger blast radius if a batch fails (all documents in the batch fail)

### Batch Timeout

The timeout ensures documents are flushed even when volume is low:

```hocon
indexer {
  batchTimeout: 5000  # milliseconds; default is 100
}
```

A higher timeout allows more documents to accumulate (better throughput) but increases the time between a document being processed and appearing in the index.

### Recommended Starting Points

| Scenario | batchSize | batchTimeout |
|---|---|---|
| High-volume batch ingest | 500–2000 | 5000–10000 ms |
| Low-volume incremental | 50–100 | 100–500 ms |
| Streaming (low latency) | 50–100 | 100 ms |
| Large documents (>1MB each) | 10–50 | 1000 ms |

---

## Scaling in Distributed Mode

### Adding Workers

In Kafka-distributed mode, adding Worker processes increases throughput proportionally (up to the number of source topic partitions):

```bash
# Start additional Worker processes
java -Dconfig.file=config.conf -cp 'target/lib/*' com.kmwllc.lucille.core.Worker my-pipeline
```

Each Worker joins the same Kafka consumer group. Kafka distributes partitions automatically.

**Maximum parallelism** = number of partitions on the source topic. If you have 8 partitions, at most 8 Workers can consume concurrently. To scale beyond this, increase the partition count on the source topic.

### Adding Workers While Running

Workers can be added to a running ingest. The new Worker joins the consumer group, triggers a rebalance, and begins consuming from assigned partitions immediately. No restart required.

### WorkerIndexer for Reduced Overhead

If the Indexer is not a separate bottleneck, use WorkerIndexer to avoid the Kafka round-trip between Worker and Indexer:

```bash
java -Dconfig.file=config.conf -cp 'target/lib/*' com.kmwllc.lucille.core.WorkerIndexer my-pipeline
```

Each WorkerIndexer process pairs one Worker with one Indexer, communicating via an in-memory queue. Scale by adding more WorkerIndexer processes.

---

## Backpressure Tuning

### Local Mode: Queue Capacity

```hocon
publisher {
  queueCapacity: 10000  # default
}
```

This bounds the in-memory processing and indexing queues. If the Connector publishes faster than Workers consume, `publish()` blocks when the queue is full. Increasing this allows more documents to buffer in memory (higher throughput burst capacity) at the cost of memory.

### Distributed Mode: Max Pending Docs

```hocon
publisher {
  maxPendingDocs: 80000
}
```

This blocks the Connector when too many documents are in flight (published but not yet terminal). Without this, a fast Connector can publish millions of documents onto Kafka before Workers process them, consuming Kafka storage and potentially causing consumer lag issues.

**Rule of thumb**: Set `maxPendingDocs` to roughly 2–5× the number of documents your Workers can process per minute.

---

## Memory Tuning

### Heap Sizing

Always set `-Xmx` explicitly:

```bash
java -Xmx4g -Dconfig.file=config.conf -cp 'target/lib/*' com.kmwllc.lucille.core.Runner
```

### Memory Budget

| Component | Memory Driver |
|---|---|
| Worker threads | Stage resources × number of threads (models, connections, caches) |
| Queue capacity | `queueCapacity` × average document size |
| Indexer batch | `batchSize` × average document size |
| Document overhead | Jackson ObjectNode per document in flight |

### Symptoms of Insufficient Memory

- Frequent GC pauses (visible as throughput drops in periodic stats)
- `OutOfMemoryError` (fatal — process crashes)
- Throughput that decreases over time (GC pressure increasing as heap fills)

### Reducing Memory Usage

- Reduce `worker.threads` (fewer model copies)
- Reduce `publisher.queueCapacity` (fewer buffered documents)
- Reduce `indexer.batchSize` (smaller batches in memory)
- Use singleton pattern for large shared resources (see Quick Reference: Threading Model)
- Delete large fields (e.g., `file_content`) after extraction stages are done with them

---

## Optimizing Specific Stages

### Stages That Call External Services

Stages that make HTTP calls, query databases, or call APIs are typically I/O-bound. Adding Worker threads helps because threads can overlap their I/O waits:

```hocon
worker {
  threads: 16  # high thread count for I/O-bound pipelines
}
```

### Stages That Load Models

Stages that load ML models (JLama, OpenNLP) are memory-bound. Each thread loads its own model copy. Consider:
- Using the singleton pattern (`DictionaryManager` approach) for thread-safe models
- Reducing thread count to fit within memory
- Using a smaller/quantized model

### Stages That Generate Children

Stages like `ChunkText` that generate many children per document can create throughput spikes. The lazy iterator model prevents memory issues, but the downstream stages must process all children before the next parent document is pulled. If children are expensive to process (e.g., embedding generation per chunk), the effective throughput per parent document is:

```
time_per_parent = pipeline_latency × (1 + number_of_children)
```

To improve this: reduce chunk count (larger chunks), or add more Worker threads to parallelize across parents.

---

## Kafka Tuning for Distributed Mode

### maxPollIntervalSecs

```hocon
kafka {
  maxPollIntervalSecs: 600  # 10 minutes
}
```

This is the maximum time between Kafka `poll()` calls before the consumer is evicted from the group. If a single document takes longer than this to process, the Worker is kicked from the group and the document is redelivered to another Worker.

**Set this higher than your slowest expected document processing time.** If you have stages that call slow external APIs or process very large documents, increase this value.

### maxRequestSize

```hocon
kafka {
  maxRequestSize: 250000000  # 250MB
}
```

The maximum size of a single Kafka message. If your documents are large (e.g., containing extracted PDF text or binary content), this must be large enough to hold the largest document after JSON serialization.

### Partition Count

More partitions = more potential parallelism. The source topic's partition count is the upper bound on Worker parallelism.

**Rule of thumb**: Set partition count to at least 2× the maximum number of Workers you expect to run. This gives room to scale without repartitioning.

---

## Monitoring Performance Over Time

### Key Metrics to Watch

During a run, track these from the periodic log messages:

1. **One minute rate** (docs/sec) — is it stable, increasing, or decreasing?
2. **"Waiting on" count** — is it at the max (backpressure active) or low (Connector is the bottleneck)?
3. **Mean pipeline latency** — is it stable or increasing over time? (Increasing suggests memory pressure or external service degradation)
4. **Mean backend latency** — is the search engine keeping up?

### Signs of a Healthy Run

- One minute rates are stable after the initial ramp-up period (~60 seconds)
- "Waiting on" count is between 0 and `maxPendingDocs` (not pegged at either extreme)
- Pipeline latency is stable
- No ERROR messages in logs

### Signs of Trouble

- One minute rate decreasing over time → memory pressure or external service degradation
- "Waiting on" pegged at max → pipeline or indexer is the bottleneck
- "Waiting on" at 0 → connector is the bottleneck (or it's finished)
- Pipeline latency increasing → GC pressure, external service slowing down, or resource exhaustion
- Indexer showing 0 docs indexed → backend unreachable or all documents being dropped before reaching indexer

---

## Performance Checklist

- [ ] Consider whether local mode is sufficient before adopting distributed mode
- [ ] If running multiple pipelines, decide whether they should be sequential (single config) or parallel (separate configs)
- [ ] Identify the bottleneck (Connector, Pipeline, or Indexer) from periodic log messages
- [ ] Check per-stage metrics to find the slowest stage
- [ ] Use conditional execution (`conditions` blocks) to skip expensive stages for documents that don't need them
- [ ] Remove unnecessary fields early — ideally don't populate them in the Connector; otherwise use `DeleteFields` at the start of the pipeline
- [ ] Consider whether large data (file content, binary blobs) needs to live on the document or whether a path/URL reference is sufficient
- [ ] Delete large intermediate fields (e.g., `file_content`) immediately after the stage that needs them
- [ ] Set `worker.threads` appropriate for your pipeline's CPU/memory profile
- [ ] Set `indexer.batchSize` and `batchTimeout` for your backend's optimal batch size
- [ ] Set `-Xmx` heap size with headroom for GC
- [ ] In distributed mode: ensure source topic partition count >= desired Worker count
- [ ] Set `kafka.maxPollIntervalSecs` higher than your slowest document processing time
- [ ] Set `publisher.maxPendingDocs` or `publisher.queueCapacity` to prevent memory exhaustion
- [ ] Monitor for increasing latency over time (indicates resource pressure)

