---
title: The Problem of Search ETL
weight: 1
date: 2025-06-09
description: >
  Why a simple sequential loop falls short for production search ingestion, and how Lucille addresses the pitfalls.
---

Getting data into a search engine involves three distinct functions:

1. Connect to a source system to acquire data.
2. Clean and enrich the data.
3. Send the data to the search backend.

In a first prototype of a search ingestion process, it is common to perform these functions in a sequential loop:

```java
// as long as the source system has more data for us to consume…
while (source.hasNext()) {
    // 1 – create a Document from the next record
    Document doc = Document.from(source.next());
    // 2 – pass the Document through an enrichment pipeline
    pipeline.process(doc);
    // 3 – send the document to the search engine
    searchClient.index(doc);
}
```

When you're ingesting a few hundred documents in a POC, this loop finishes in seconds. It works. It's easy to reason about. At this point, adopting a search ETL framework feels like overkill.

Then you go to production.

The dataset is no longer a few hundred documents — it's millions. The loop that finished in ten seconds now takes a full day. And over the course of that day, you run into a series of problems that the simple loop has no answer for.

---

### 1. Error Handling

Somewhere in that day-long run, a document fails. Maybe the enrichment logic throws an exception on an unexpected input. Maybe the search backend rejects a malformed field. The loop stops.

Do you fix the document and restart from the beginning? That means re-ingesting everything you already indexed. Do you build checkpointing logic so you can resume from where you left off? Now you need a persistent state store. Or do you skip the bad document and keep going? If you do, where does the failed document go? How do you know at the end of the run which documents succeeded and which didn't? How do you inspect and reprocess just the failures?

The simple loop gives you no framework for any of these decisions. And search backends make this harder than general ETL in a specific way: bulk indexing APIs return mixed results. A single bulk request can partially succeed — some documents accepted, others individually rejected due to a field type mismatch or a mapping conflict — and the response requires per-document inspection to know what actually happened. You can't treat indexing as a simple pass/fail operation and retry the batch whole. You need per-document error tracking inside every batch.

---

### 2. Observability

The run has been going for six hours. Is it on track? Is it stuck? Are there bottlenecks building up somewhere? How many documents have been indexed so far, and how many are still waiting?

The loop has no instrumentation. You have no visibility into throughput, no per-stage latency breakdown, no count of in-flight documents, no way to distinguish "slow but progressing" from "silently stuck." You're watching a process run for a day with no dashboard and no progress bar.

---

### 3. Pipeline Complexity and Reuse

As the pipeline grows — adding text extraction, NLP, embedding generation, database lookups — the enrichment logic accumulates inside or alongside the loop. It becomes difficult to adjust individual steps without touching the whole thing. Testing a change requires running the full ingest or building a separate harness.

When a second project needs some of the same transformations, there's no clean mechanism for reuse. The logic gets copied or extracted into a shared library that gradually accumulates its own complexity. Configuration for which steps run and in what order is hardcoded rather than declared.

---

### 4. One Source Document, Many Search Records

In general ETL, a record in the source maps to a record in the destination. Search ingestion breaks that assumption.

For semantic search and RAG pipelines, a single source document — a PDF, a support ticket, a product page — often needs to be split into smaller chunks, each indexed as an independent search record so that retrieval can return the most relevant passage rather than the entire document. One source document becomes tens or hundreds of index records.

The simple loop has no model for this. `searchClient.index(doc)` indexes one document. If a stage in the pipeline splits a document into chunks, those chunks need to flow through the rest of the pipeline independently, be tracked individually for accounting purposes, and be indexed as separate records. The parent document and its chunks may also need to be indexed in a specific order if the chunks reference the parent by ID.

This 1-to-N fan-out is unique to search ingestion. It doesn't arise in database ETL, and it breaks both the loop's execution model and any completion-tracking logic built on top of it.

---

### 5. Configuration and Environment Management

The pipeline runs against a development search backend today. Next week it needs to run against staging, with different credentials, a different Solr URL, and a different collection name. After that, production — with its own connection details, possibly different Kafka brokers, and secrets that shouldn't be hardcoded.

Managing that across environments — without duplicating code, without accidentally running dev config against prod, with a clear way to inject secrets at runtime — is not a problem the loop was designed to solve.

---

### 6. Scalability

This is the hardest problem, and the reason all the others matter more once you solve it.

If a full dataset ingest takes a day, the obvious question is: can we make it faster? The answer is yes — but not by iterating on the sequential loop. To go faster, you have to parallelize.

Part of what makes this problem harder in search ingestion than in general ETL is the cost of the enrichment step itself. In database-to-database ETL, transformation is often lightweight — type conversion, field mapping, simple lookups. In search ingestion, enrichment exists specifically to pre-compute things that make search fast and relevant: OCR on scanned documents, named entity extraction, classification, vector embedding generation. This work is genuinely expensive per document. The pipeline is CPU-bound in a way that most ETL pipelines are not, which is exactly why parallelizing it matters so much — and why the gains from doing it correctly are so large.

The loop is slow because its three steps have fundamentally different performance characteristics: connecting to the source system is I/O-bound, pipeline enrichment is often CPU-bound (model inference, API calls, computation), and indexing is I/O-bound but also depends on the CPU of the search backend. The sequential loop adds the latency of all three steps per document:

```
total time = ((source latency) + (pipeline latency) + (indexing latency)) × (number of documents)
```

To do better, you need to run these three phases concurrently — reading the next batch of documents while the previous batch is being enriched, while the batch before that is being indexed. And you need multiple enrichment workers running in parallel to saturate available CPU.

But parallelizing the loop creates its own roadblocks. Multiple reader threads need to coordinate so they don't fetch the same records twice. The enrichment stage may hold expensive resources — models, connection pools — that can't safely be shared across threads. The search backend performs best when documents arrive in batches, not one at a time, which requires accumulating results from multiple parallel workers before flushing. And if you scale to multiple machines, you need a message queue to distribute work across processes, which means managing Kafka topics, consumer groups, and partition assignment.

Then, once you succeed in parallelizing, a new set of problems emerges. How do you know when the run is complete? With a sequential loop, the answer is obvious: the loop returns. With parallel workers, you can't know the run is done just because the source is exhausted — documents are still in flight, being processed and indexed. You need an accounting system that tracks every document from publication through its terminal state. How do you handle backpressure? A fast source feeding slow enrichment workers will produce documents faster than they can be consumed, eventually exhausting memory. How do you handle a worker crash mid-run? Kafka's consumer group protocol can reassign its partitions, but unacknowledged documents need to be redelivered without silently dropping or double-indexing them. What about a document that repeatedly crashes a worker — a malformed input that triggers a bug in third-party code? Without a retry limit and a dead-letter queue, one bad document can loop forever.

---

Lucille is the system you arrive at after encountering these problems in real production deployments, solving them one by one, and validating the solutions at scale. It is not a theoretical framework — it is the accumulated result of decisions forced by real failure modes.
