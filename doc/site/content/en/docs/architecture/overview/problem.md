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

When you're ingesting a few documents as a POC, a loop like this gets the job done in a few lines of code. At this point, adopting a search ETL framework — with its own learning curve and dependencies — seems like overkill.

Over time, as a POC turns into a production system, this simple loop almost always falls short. The question becomes: do you want to keep making this loop more complex with custom code, or does it make sense to use a framework that has proven successful in real-world deployments?

### Pitfalls of the Simple Loop

**Scalability.** The sequential loop doesn't scale. During a POC, the difference between a 10-second and 20-second ingest might not seem significant. At scale, the difference between a 1-day and 2-day ingest is enormous. When you're trying to ingest huge volumes of data, you need to do everything possible to improve throughput.

The loop is slow because its three steps have fundamentally different performance characteristics: connecting to the source system is I/O-bound, pipeline enrichment is often CPU-bound (model inference, API calls, computation), and indexing is I/O-bound but also depends on the CPU of the search backend. The sequential loop adds the latency of all three steps per document:

```
total time = ((source latency) + (pipeline latency) + (indexing latency)) × (number of documents)
```

To improve this, we want to take advantage of available CPU and I/O capacity to process documents in parallel. But you can't easily parallelize the tight loop — multiple instances would need to coordinate so they don't read the same records from the source system. Furthermore, indexing performs best when documents are sent to the backend in batches, not one at a time.

**Error handling.** You need a way to track errors and distinguish fatal failures from recoverable per-document failures. A single malformed document shouldn't abort a run processing millions of records.

**Observability.** You need a way to monitor the pipeline and understand which stages are bottlenecks.

**Reuse.** You don't want to rewrite common transformations — text extraction, entity recognition, embedding generation — for every project.

---
