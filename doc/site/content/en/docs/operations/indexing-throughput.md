---
title: "Indexing Throughput"
weight: 6
date: 2026-10-02
description: >
  How indexing throughput is limited, and how to raise it in graded steps: larger batches, more
  consumers, and more sending threads per consumer.
---

This page is for when **indexing** is the bottleneck: documents reach the search backend more slowly than the pipeline produces them, even though the backend could accept them faster. It covers the levers for closing that gap. If the backend itself is at capacity, that is a separate problem (scaling or tuning the search cluster is out of scope here). To find out whether the Connector, Pipeline, or indexing is the bottleneck in the first place, start with [Performance Tuning]({{< relref "docs/operations/performance-tuning" >}}), then come back here once you have established that indexing is the limiting step.

## What limits indexing throughput

An Indexer accumulates documents into batches and sends each batch to the destination as one bulk request. With the default settings it has **one bulk request in flight at a time**: it sends a batch, waits for the response, then sends the next. On many deployments the indexer may spend the majority of its wall-clock time *blocked waiting for the destination to respond*, not building requests.

That matters because the search backend can often absorb more than a single sequential client delivers. The clients for OpenSearch, Elasticsearch, and Solr may not, on their own, saturate the server from a sequential send loop — even with a well-tuned batch size. So at a tuned batch size the limit is often neither the batch size nor the backend's capacity, but the fact that one client sends one request at a time and the destination sits partly idle between requests.

There are three levers for raising indexing throughput, in rough order of how much they change and how far they reach:

1. **Larger batches** — make each bulk request carry more documents.
2. **More consumers** — run more Kafka consumers (more Indexer or WorkerIndexer threads and/or processes), each sending its own bulk requests concurrently with the other consumers.
3. **More sending threads per consumer** — let a single consumer keep several of its own bulk requests in flight at once, by running its sends on *its own* small pool of threads.

The sections below take them in that order. A later [Choosing and combining the levers](#choosing-and-combining-the-levers) section covers how they relate and when to reach for each; levers 2 and 3 are not alternatives — they compose.

## Lever 1: Larger batches

Larger batches amortize per-request overhead (HTTP round trips, request parsing, backend refresh churn) over more documents.

```hocon
indexer {
  batchSize: 500      # documents per bulk request; default 100
  batchByteSize: 5000000  # optional; flush when the batch reaches this many bytes
  batchTimeout: 1000  # ms since the last add/flush before the batch is flushed anyway; default 100
}
```

- `batchSize` (default **100**) and `batchByteSize` (default unset) bound a batch by count and/or bytes; if both are set, the batch flushes when either limit is reached.
- `batchTimeout` (default **100 ms**) flushes a partly-filled batch when volume is low, so documents are not left waiting.

Batch tuning has a ceiling: a bigger batch amortizes overhead but does **not** overlap the wait for the destination. Once overhead is amortized, raising the batch size further stops helping, and the sequential send-wait-send pattern remains. To go past that, overlap the sends — by adding consumers, by adding concurrency within a consumer, or both.

See the batching table in [Performance Tuning]({{< relref "docs/operations/performance-tuning" >}}) for recommended starting points by workload.

## Lever 2: More consumers

In Kafka-backed modes (distributed and hybrid), you scale by adding Kafka consumers, each of which owns a dedicated set of partitions. A consumer still sends its own documents sequentially, one bulk request at a time, but different consumers run independently, so each additional consumer raises the number of bulk requests in flight against the destination. Which component is the consumer, and which topic it reads, differs by mode:

- **Distributed mode.** The standalone indexer *is* the Kafka consumer: it reads the **destination topic** and sends what it polls to the backend. Adding indexer processes adds indexing capacity alone, without multiplying the pipeline, so this is the cleaner way to scale *only* the indexing side.
- **Hybrid mode.** The indexer does *not* read Kafka; it polls an in-JVM queue fed by its paired worker. The **worker** is the Kafka consumer, reading the **source topic**. A `WorkerIndexer` pairs one worker with one indexer, so adding pairs (`worker.threads`, or more processes) adds indexers, but it *also* adds pipeline workers with their per-worker state, models, and memory. That is the right lever when the pipeline needs to scale too, and wasteful when the pipeline already keeps up and only the indexer is behind.

### The partition ceiling

All consumers form a single Kafka consumer group, and Kafka assigns each partition to **at most one** consumer in the group. So the number of consumers that can be *active* at once is capped at the partition count of the topic they read: the **destination topic** in distributed mode, the **source topic** in hybrid mode. On a topic with W partitions, the W+1th consumer sits idle with no partition assigned.

This means **adding consumers may first require adding partitions.** If you are at "W consumers, W partitions," you cannot get more send parallelism from more consumers without repartitioning that topic, an operational change that affects every consumer and redistributes keys. Partition count is therefore the first thing to check when consumer-scaling stops helping.

### Ordering is preserved

Adding consumers does not reorder operations on a document. The source and destination topics are keyed by `document.getId()`, so all operations on a given document id route to the same partition and therefore the same consumer, which processes them in order.

> **Caveat (`idOverrideField`).** If `idOverrideField` is set, the destination id can differ from the Kafka message key (the document id). Two documents with different document ids but the same override id are keyed to possibly-different partitions and can reach different consumers; ordering between them is then not guaranteed. This is pre-existing and independent of the throughput levers.

## Lever 3: More sending threads per consumer

Lever 2 adds consumers, but each one still sends **one bulk request at a time**. This lever overlaps sends *within* a single consumer: `indexer.maxConcurrentBatches` (K, default **1**) lets one consumer keep up to K bulk requests in flight at once — multithreading for bulk sends. It needs no additional consumers and no additional partitions.

```hocon
indexer {
  maxConcurrentBatches: 4   # up to 4 bulk requests in flight at once; default 1
}
```

At `maxConcurrentBatches: 1` (the default) each consumer sends one batch at a time, synchronously. At K > 1, each consumer runs the sends on its own pool of K threads, completing them in dispatch order.

### Choosing K

The benefit is bounded by how much idle time there is to reclaim between sequential sends and by the backend's true capacity. Where the destination has headroom, overlapping K sends moves throughput from one client's sequential ceiling toward what the server can actually absorb, with diminishing returns as K rises (client-side CPU per document grows with K). **K ≈ 4 is a reasonable starting point;** the right value depends on the destination's shards, replicas, and write capacity. Treat the destination, not the client, as the thing you are trying to saturate — raising K past the point where the backend is saturated only adds client-side cost and more `429`/throttling responses.

### Requirements and guarantees

Concurrency is only enabled when it is safe, and the dispatcher upholds ordering and accounting while sends overlap:

- **The destination client must be thread-safe.** Solr, OpenSearch, Elasticsearch, and the Nop indexer support K > 1; others reject `maxConcurrentBatches > 1` at startup. (A custom indexer opts in by declaring its client thread-safe.)
- **Same-document ordering is preserved.** The dispatcher never lets two batches that touch the same document — or its children, recursively — be in flight at the same time, so a write and a delete of the same document are still applied in order even though sends overlap. This is the key property that distinguishes concurrent batches from naively threading the send loop.
- **Delete-by-query runs alone.** A batch containing a delete-by-query is sent only when nothing else is in flight, and nothing is dispatched behind it until it completes (see [Delete-by-query and single-partition topics](#delete-by-query-and-single-partition-topics) below).
- **Completion stays in dispatch order.** Batches finish sending out of order but are completed (events emitted, offsets committed) in the order they were dispatched, so a messenger that commits its input on completion never commits past an unfinished batch.

### Connection pools (OpenSearch / Elasticsearch)

K concurrent sends need at least K connections to the destination, or the requests would queue at the connection layer and erase the gain. The OpenSearch and Elasticsearch clients size their HTTP connection pools from `maxConcurrentBatches` automatically:

- per-route connections default to **`max(5, K + 1)`** (one spare for pings and other requests),
- total connections default to **`max(25, per-route)`**.

Override only if needed:

```hocon
opensearch {       # or: elasticsearch { ... }
  maxConnectionsPerRoute: 10
  maxConnectionsTotal: 50
}
```

Solr's client (SolrJ) manages its own connection pool and is not sized from `maxConcurrentBatches`; a high K against Solr may hit SolrJ's internal connection limits.

## Choosing and combining the levers

Levers 2 (more consumers) and 3 (`maxConcurrentBatches`) both increase the number of bulk requests in flight, but at different granularities — across partitions versus within a consumer — and they are **not interchangeable**. They are, however, **compatible and composable**: N consumers each sending K batches concurrently gives up to **N × K** bulk requests in flight at once (see [Fan-out](#fan-out-in-hybrid-and-distributed-modes) for how that multiplies within a single JVM). The decisive differences when choosing between — or combining — them:

- **The partition ceiling.** More consumers is capped at the partition count and may require repartitioning to grow (see [Lever 2](#lever-2-more-consumers)). `maxConcurrentBatches` adds concurrency inside each existing consumer and needs no new partitions — so it is the lever available when you are already at the partition ceiling.
- **Granularity and cost.** `maxConcurrentBatches` is a per-indexer number, tuned without changing deployment topology. More consumers is a coarser, operational change: more standalone indexers means more JVMs, clients, consumers, and connection pools; more WorkerIndexer threads additionally multiplies the pipeline.
- **Local mode.** There is a single indexer and no Kafka, so `maxConcurrentBatches` is the only lever that overlaps sends.
- **Delete-by-query.** Should be run with a single active indexer (single-partition topic), which rules out more consumers. Larger batches (Lever 1) still apply, but `maxConcurrentBatches` is the *only* lever that can overlap sends in that case — see below.

A deployment that can add partitions and consumers can reach comparable send throughput without `maxConcurrentBatches`. Its value is for deployments that are indexer-bound at or near the partition ceiling, that want footprint to stay proportional to the bottleneck rather than multiplying consumers, or that fall into the delete-by-query case.

## Delete-by-query and single-partition topics

This is the case where concurrent bulk sends are not just the cheaper option but the **only way** to overlap sends.

A delete-by-query deletes whatever matches a *query* at the destination — not a known set of document ids. Within one indexer, the dispatcher makes a delete-by-query batch run alone, so no write on that indexer overlaps it. But that barrier is **per-indexer**; it cannot coordinate across indexers. With more than one partition — and therefore more than one indexer — one indexer could run a delete-by-query while another is concurrently writing a document the query matches, a race nothing prevents. (Document-id keying does not help here: a delete-by-query interacts with documents by query match, not by id, so routing same-id operations to one consumer is irrelevant — the query can match ids that live on other partitions entirely.)

For that reason, **a delete-by-query ingest should be run with a single active indexer**, which in Kafka mode means a single-partition gating topic (one partition → at most one consumer in the group → one indexer). The gating topic is the **destination topic** in distributed mode and the **source topic** in hybrid mode (one active worker → its one paired indexer).

> **This is an operational constraint, not something Lucille enforces.** Lucille does *not* check the partition count or the number of running indexers when delete-by-query is configured — there is no startup validation and no runtime guard for it. (Lucille validates only that `indexer.deleteByFieldField` and `indexer.deleteByFieldValue` are set together, and likewise `indexer.deletionMarkerField` and `indexer.deletionMarkerFieldValue`; it checks nothing about partitions or the number of indexers.) So nothing warns you if you run a delete-by-query ingest with multiple partitions or multiple indexers. You would not get an error; you would get **incorrect deletes** — a delete-by-query on one indexer racing a concurrent write on another, deleting or sparing documents inconsistently — and you would typically only discover it by observing wrong results at the destination. Treat single-active-indexer as a configuration you must arrange yourself. The constraint holds regardless of `maxConcurrentBatches`, and even a plain sequential indexer is unsafe run as multiple instances against delete-by-query.

The throughput consequence is decisive: with one partition, adding WorkerIndexer threads or standalone indexers yields **zero** send parallelism — the extra consumers sit idle. Within that one indexer, batch size (Lever 1) still applies, but **`maxConcurrentBatches` is the only lever that can overlap bulk sends,** and it stays safe because the delete-by-query still runs alone and the surrounding writes stay ordered, all within the single indexer that owns the whole corpus.

(The operative requirement is "one active indexer"; a single-partition topic is the standard way to arrange it. The narrow exception — a partitioning under which every delete-by-query only ever matches documents on its own partition — is not something Lucille arranges or can assume, so single-partition is the correct general guidance.)

## Fan-out in hybrid and distributed modes

`maxConcurrentBatches` (K) is **per indexer**. In hybrid mode (`WorkerIndexer`), each worker-indexer pair has its own indexer, and a single JVM runs `worker.threads` (or the pipeline's `threads`) of them — so K multiplies by the number of pairs within one process. With W pairs, a hybrid JVM runs up to:

- **W × K** send threads, and up to **W × K** batches in flight at once — that many built request bodies held in memory simultaneously (so memory, not just thread count, scales this way); and
- for OpenSearch/Elasticsearch, **W × max(5, K + 1)** connections per route and **W × max(25, max(5, K + 1))** total, since each pair's indexer sizes its own pool independently.

Two things to size for:

1. **Choose K against the consumer count, not in isolation.** The real send concurrency and connection load against the destination from one JVM is W × K (and more if several such JVMs run). A modest per-indexer K can still be a large aggregate — e.g. `worker.threads: 8` with `maxConcurrentBatches: 4` is 32 concurrent sends and 40+ connections from one process. Compare W × K to the destination's write/shard capacity.
2. **The "at most K in flight" limit is per indexer, not per JVM.** Each pair enforces its own window; there is no process-wide cap.

Distributed mode has the analogous fan-out across *processes*: each standalone indexer process contributes its own K. Local mode runs a single indexer, so K is the whole story.

## Delivery semantics (Kafka, distributed mode)

In distributed mode, the standalone Kafka indexer commits destination-topic offsets **on batch completion** — after a batch's documents have been indexed and their events emitted — rather than at poll time. This makes delivery **at-least-once**: a crash or consumer-group rebalance never silently drops in-flight documents. Documents that were polled but not yet committed are **re-delivered** (and re-indexed) after a restart. Re-indexing is safe because search-engine upserts are idempotent.

A consequence to be aware of: a re-delivered document emits a second completion event, so a run's reported success count can exceed the number of distinct documents after a redelivery. This does not stall or hang the run.

> Hybrid mode already commits on completion (the indexer hands offsets to its worker, which commits them), so this is not a change for hybrid deployments. Local mode has no Kafka and no offset commits.

If you supply a Kafka consumer configuration through `kafka.consumerPropertyFile`, it **must** set `enable.auto.commit=false`. Auto-commit would commit offsets at poll time — before documents are indexed — reintroducing an at-most-once window in which a crash could skip documents. The indexer (and worker) reject a document consumer that enables auto-commit, or that leaves it unset (kafka-clients defaults it to `true`), with a startup error. Deployments using the normal inline Kafka configuration are unaffected; Lucille sets `enable.auto.commit=false` for them.

## Implementation details: concurrent bulk sends

This section covers how the third lever — concurrent bulk sends within a consumer — is implemented. (Larger batches and more consumers are straightforward; the subtlety is specific to overlapping sends within one indexer.)

Running `sendToIndex` on a thread pool is only the easy part. The moment sends overlap, correctness constraints appear, and most of the implementation exists to honor them. They fall into three tiers: **primary** (what reaches the destination must be correct), **secondary** (the accounting and liveness around each send must stay correct), and **tertiary** (details that make it safe to adopt and maintain). This is a map of those considerations for readers who want to understand how it works rather than how to configure it.

### Primary — correctness of what reaches the destination

These are covered above; restated briefly:

- **Same-document ordering** — two batches that write the same destination id (a document's own id, its `idOverrideField` value, and recursively its children's ids) are never in flight together, so a write and a delete of the same document cannot race (see [Lever 3](#lever-3-more-sending-threads-per-consumer)).
- **Delete-by-query is a barrier** — it matches documents by query, not by a known id set, so it runs alone (see [Delete-by-query and single-partition topics](#delete-by-query-and-single-partition-topics)).

### Secondary — accounting and liveness around the send

- **Ordered completion.** Sends finish in whatever order the destination responds, but a batch is *completed* — its FAIL/FINISH events emitted and its input marked done — strictly in dispatch order. Completing a later batch while an earlier one is still unfinished would mark not-yet-finished work as done, so a crash at that instant could skip the earlier batch's documents. The dispatcher holds a finished-but-not-yet-completable batch until all its predecessors have completed. (This is what makes commit-on-completion safe; see also [Message Ordering]({{< relref "docs/architecture/internals/message-ordering" >}}).)
- **Stale offsets after a rebalance.** With commit-on-completion, a batch can finish *after* its partition was revoked and reassigned to another consumer that has since committed further. Committing that batch's now-stale offset would rewind the new owner. Each partition carries an assignment *generation*; a polled document is stamped with the generation it was delivered under, and its offset is committed only if that generation is still current — so a stale completion is dropped rather than committed, and a committed offset never moves backward. The committed offset is a *frontier* that advances across the sequence of offsets actually polled, so out-of-order completion never commits past an unfinished earlier record, and a gap in the polled offsets does not stall it.
- **Consumer-group liveness during long waits.** While the indexer thread blocks waiting for in-flight batches, it is not polling Kafka, and a consumer that stops polling is evicted from its group. A `keepAlive` hook lets the messenger contact the broker during those waits — paused, delivering nothing and not moving its read position — purely to stay a live group member.
- **Interrupt safety.** An interrupt from a forced or timed-out shutdown can arrive while the indexer thread is waiting on an in-flight send. The batch has already been sent and must still be accounted for, so the wait absorbs the interrupt rather than abandoning the batch; completion then runs with the interrupt flag cleared (its messenger calls may themselves block), and the flag is restored afterward so the run still exits. This holds at `maxConcurrentBatches = 1` as well as higher.
- **Batch-timer accounting.** While the indexer thread is blocked sending (or waiting for a free slot, or committing a finished batch) it cannot add documents, yet the current batch's expiry timer keeps running. Left alone, the next add would see an "expired" batch and flush a one-document bulk. Time spent blocked is therefore excluded from the batch timer — which also helps at `maxConcurrentBatches = 1`, after a slow send.

### Tertiary — safe to adopt and maintain

- **Safe-by-default gates.** Two independent assumptions must hold for concurrency to be safe: the destination client is thread-safe, and the messenger commits its input on completion rather than at poll. Each is an opt-in check that defaults to the safe answer — `supportsConcurrentSends()` on the indexer and `commitsOnBatchCompletion()` on the messenger — so an unknown indexer or messenger disables concurrency rather than risking corruption or data loss.
- **The auto-commit guard** (see [Delivery semantics](#delivery-semantics-kafka-distributed-mode)) closes the one way a Kafka consumer could silently defeat commit-on-completion.
- **Connection-pool sizing** from `maxConcurrentBatches` (see [Lever 3](#lever-3-more-sending-threads-per-consumer)) keeps the HTTP layer from serializing the overlapped sends.
- **A dispatcher seam keeps `Indexer` readable.** The run loop is identical regardless of concurrency: it hands batches to a `BatchDispatcher`. `SynchronousDispatcher` is the default (send and complete inline on the indexer thread); `ConcurrentDispatcher` owns the thread pool, the in-flight window, the ordering gate, and the barrier. `Indexer` supplies the domain meaning (what a destination id is, what a delete-by-query barrier is, how a send retries) and the dispatcher owns only scheduling; Kafka offset bookkeeping is isolated in a dedicated tracker. The in-flight state is confined to the indexer thread, so it needs no locking.

## Quick reference

| Lever | Config | Effect | When |
|---|---|---|---|
| Larger batches | `indexer.batchSize` (default 100), `batchByteSize`, `batchTimeout` (default 100 ms) | More documents per bulk request | Always first; amortizes per-request overhead |
| More consumers | `worker.threads` (hybrid); more standalone Indexer processes (distributed) | Overlap sends across partitions | Partitions available (or add them) — source topic in hybrid, destination topic in distributed; WorkerIndexer also scales the pipeline |
| Concurrent sends | `indexer.maxConcurrentBatches` (default 1) | Overlap K bulk requests within one indexer | Indexer-bound with destination headroom; at/near the partition ceiling; local mode; delete-by-query (single partition) |
| Connection pool | `opensearch`/`elasticsearch` `.maxConnectionsPerRoute` / `.maxConnectionsTotal` | Override the K-derived pool size | Rarely; defaults track K |
