---
title: What Is Search ETL?
weight: 1
date: 2026-11-08
description: >
  What search ETL is, why it is a distinct category of ETL problem, and why it calls for a dedicated tool.
---

Search ETL is the work of taking data from wherever it lives (databases, file shares, object storage, message streams, content systems) and turning it into documents that a search engine or vector database can index well. It shares the shape of classic ETL: extract from a source, transform, load into a destination. It also inherits classic ETL's demands for scheduling, incremental loads, environment-specific configuration and secrets, and recovery from failure.

The resemblance ends there. The destination is not a table, the transformation is not a type conversion, and the unit of work is not a row. Those three differences change nearly every engineering decision, and they are why search ETL deserves to be treated as a discipline of its own.

---

## Same Shape, Different Problem

Most ETL and ELT writing treats a search index as one more destination alongside a warehouse or a data lake. In practice, almost every assumption behind warehouse tooling breaks when the destination is an index.

| | Warehouse ETL / ELT | Search ETL |
|---|---|---|
| **Destination** | A table, written with inserts or merges | An index with its own bulk protocol, partial failures, ordering, routing and versioning |
| **Unit of work** | A row | A document, which often fans out into many chunks or child records |
| **Transformation** | Mostly cheap: type conversion, joins, mapping, often done in SQL after loading | Expensive enrichment, mostly done before loading: text extraction, OCR, NLP, embeddings |
| **Change** | Append or merge | Update in place, delete, and clean up every record derived from a deleted source |
| **Completion** | The job exits | Every in-flight document and child reaches a known terminal state |
| **Iteration** | Occasional schema migrations | Constant relevance tuning: field mappings, chunk sizes, extracted entities |

ELT's central idea, load the raw data first and transform it where it lands, fits poorly. Search engines do offer processing at ingest time, such as ingest pipelines and update processors, and they suit light, engine-specific steps. But that work runs on the search cluster's CPU, alongside your users' queries, and is limited to what the engine provides. Unlike a warehouse table, an index cannot be re-transformed later with a query: changing how documents are processed means reprocessing and reindexing them. So the heavy work that makes a document findable is best done before it arrives.

---

## Where Search ETL Diverges

**The unit of work is a document, and one input becomes many outputs.** A PDF, a product page or a support ticket is split into chunks, pages or child records so that retrieval can return the relevant passage rather than the whole file. Each derived record needs a stable identity so that re-ingestion updates rather than duplicates, a lifecycle of its own, and a link to its parent. Deleting the source must delete every derived record, and a shortened document must not leave stale high-numbered chunks behind. Row-oriented tools have no model for this, and the failure is silent: orphaned chunks stay retrievable and nothing reports them.

**Enrichment is the work, and it is expensive and fragile.** In warehouse ETL, transformation is cheap. In search, it exists to precompute what makes retrieval fast and relevant: text extraction from binaries, OCR, language detection, entity recognition, classification, chunking, embedding generation, lookups against other systems. This is CPU-bound and often calls external models or APIs with their own rate limits and outages. It needs parallelism across many workers, and it belongs in a tier you can scale independently rather than inside the search cluster, whose CPU and memory your users' queries depend on. Because any step can throw on an unexpected input, the pipeline has to isolate failures per document and keep going, while stopping at once on structural errors that no document could survive.

**The destination has its own semantics.** Search engine bulk APIs succeed partially. Some report success for the request while burying per-item errors in the response body; others reject a whole batch because of one bad document. Operations on the same id must stay ordered, deletes may be by id or by query, and versions, routing and parent-child relationships are modelled differently by every engine. Loading is not "write the rows"; it is a protocol.

**Throughput needs concurrency that clients do not give you.** Bulk batching amortizes round trips, but a client that sends one request and waits leaves the backend partly idle, and the standard search engine clients do not saturate a server from a sequential send loop. Getting there means keeping several requests in flight while guaranteeing that two batches touching the same document are never in flight together, that delete-by-query runs alone, that connection pools are sized to match, and that a failure in one batch does not corrupt the accounting of another. This is code that is easy to start and hard to get right.

**Completion is a question, not an event.** Once reading, enriching and indexing run concurrently, "the loop returned" no longer means "the data is in". Documents are in flight, children are being created mid-pipeline, batches are pending. Knowing that a run is complete, and exactly which documents succeeded, failed or were dropped, requires a ledger that tracks every published document and every child to a terminal state. Without one, teams learn about missing data from their users.

**The scale trajectory is steep.** A search project begins as a prototype that indexes a few thousand records in seconds. In production the same project has millions of records, ingests that run for hours or days, and data arriving continuously from a message stream. It needs a full-corpus batch backfill with exact accounting and a continuous streaming feed, often at the same time, and it needs to switch between them without rewriting enrichment logic. Scaling must be an operational action, adding workers while the system runs, rather than a redesign.

**Enrichment logic is iterated constantly.** Relevance work is empirical. Teams need to change the pipeline, run it against sample data, inspect what would have been indexed, and run it again, without a compile-and-deploy cycle. That favours configuration-driven pipelines that are validated before they run, composable across environments, and testable end to end with no search engine or message broker present.

**No fixed library of transformations is ever enough.** Every corpus has a quirk. A search ETL system needs clean extension points for custom connectors, stages and indexers, plus lighter paths such as scripting for logic that does not justify a new component.

**It must be operated.** Long runs need throughput and per-stage latency metrics, per-document tracing in logs, backpressure so a fast source cannot exhaust memory, quarantine for documents that repeatedly crash a worker, graceful shutdown that drains in-flight work, and exit semantics an orchestrator can act on.

---

## Search ETL and AI Ingestion

Retrieval-augmented generation has brought a wave of tools that describe themselves as "ETL for LLMs" or "ETL for AI": parse documents, chunk them, embed them, load a vector store. These tools are a sign that the category is real. They make the same argument made here, that row-oriented ETL does not fit unstructured documents.

But vector ingestion is not a separate problem. It is the newest destination of search ETL. The fan-out, deletes, enrichment cost and accounting described above apply whether the target is a lexical index, a vector database or both. Most teams now need both: the same corpus indexed for keyword search and as embeddings for semantic or hybrid retrieval. That doubles the destinations and the cost of getting it wrong. Re-embedding a large corpus after a model change can take most of a day, and chunk-level deletes and permissions add a new class of silent error.

---

## Why It Matters

Search and retrieval sit behind the user-facing surface of most data: site search, enterprise search, and the retrieval step of every RAG application. An index is only as good as what reached it, and what reached it is only as trustworthy as the pipeline's accounting. Stale or missing documents degrade results quietly; nothing crashes.

Public engineering postmortems show the pattern. One team discovered that its indexer had silently ignored bulk failures and left about a tenth of one content type unindexed, while reporting success. Another team's full reindex took more than two weeks and reached only about ninety percent consistency before the pipeline was rebuilt. Others describe deleted items that stayed searchable because index and delete requests raced, and week-long backfills every time a field was added.

---

## Why a Dedicated Tool

Teams that treat search ingestion as ordinary ETL end up rebuilding the same machinery by hand: concurrency, batching, per-item error handling, retries, delete propagation, completion tracking. The postmortems show the same gaps every time. General-purpose data platforms are built around rows and tables; engine-bundled ingestion tools tie you to one backend; AI ingestion libraries cover the vector half of the problem and leave the rest to you.

Recognising search ETL as its own category is what lets that machinery be built once, correctly, and reused. That is what Lucille is: a framework for search ETL that handles concurrency, accounting, batching, failure isolation and scale-out, so that each project only has to supply what is specific to its data and its relevance goals.

- [Why Lucille]({{< relref "why-lucille" >}}) describes what Lucille does and when it is the right fit.
- [The Architecture Overview]({{< relref "docs/architecture/overview" >}}) shows how Lucille is designed around these problems, starting from a simple ingest loop.
