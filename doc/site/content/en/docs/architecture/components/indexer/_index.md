---
weight: 9
title: Indexer
date: 2025-06-09
description: An Indexer sends processed Documents to a specific destination.
---

## What an Indexer Does

An Indexer is the component responsible for delivering processed Documents to their final destination — typically a search engine or vector database. It is the last component in the data flow: Connectors produce Documents, Workers enrich them, and the Indexer sends them to the search backend.

## Batching

Indexers do not send documents one at a time. They accumulate documents into batches and flush them as a single bulk API call. This is essential for search engine performance — bulk writes are significantly faster than individual indexing requests, often by an order of magnitude.

A batch is flushed when any of three conditions is met: the batch reaches a configured document count (`batchSize`), it reaches a configured total size in bytes (`batchByteSize`), or a timeout (`batchTimeout`) expires since the last document was added or the batch was last flushed. The timeout ensures documents are not left waiting indefinitely in low-volume scenarios. Time the Indexer spends blocked sending is not counted toward the timeout, so a slow send does not cause the next batch to flush early with a single document.

By default an Indexer keeps one bulk request in flight at a time. It can also send multiple batches concurrently — overlapping the time spent waiting on the destination — while preserving same-document ordering and completing batches in dispatch order. This is a significant aspect of how Lucille reaches high indexing throughput; the mechanism, its guarantees, and how to tune it are covered in [Indexing Throughput]({{< relref "docs/operations/indexing-throughput" >}}).

## Single Indexer Configuration Per Run

A Lucille config defines a single Indexer: one `indexer` block, and therefore one destination and one set of batching and retry parameters, shared by every pipeline. This simplifies configuration and reflects the common reality that a search ingestion project writes to a single search backend.

That configuration can be instantiated more than once at run time. The Runner creates an Indexer for each connector's run, a WorkerIndexer JVM runs one per worker thread, and in distributed mode you can start several standalone Indexer processes per pipeline (up to the partition count of its destination topic). Each instance has its own client and connection pool and its own `maxConcurrentBatches`. See [Indexing Throughput]({{< relref "docs/operations/indexing-throughput" >}}) for how these multiply.

When documents from different pipelines need to land in different indices within the same backend, Lucille supports index routing: a field on the document determines which index it is sent to, without requiring multiple Indexer definitions.

## Deletion Support

Indexers support two deletion mechanisms — delete-by-ID and delete-by-query — triggered by marker fields on the document. This enables CDC patterns and incremental ingestion: a Connector can emit a document that represents "delete this record from the index" rather than "index this record," and the Indexer translates that intent into the appropriate backend operation.

## Field Filtering

The Indexer applies field filtering at the boundary — applying whitelist/blacklist rules, and always omitting the `___children` field — so that only the intended fields reach the search backend. Reserved fields such as `run_id` and `___skipped` are treated like any other field and reach the backend unless the whitelist leaves them out or the blacklist names them. This filtering happens at indexing time, not during pipeline processing, so Stages always see the full document.

## Error Handling at the Batch Level

Search engine bulk APIs can return mixed results: some documents accepted, others rejected. The Indexer inspects per-document responses and reports individual failures without failing the entire batch. Documents that succeed are marked complete; documents that fail are marked failed. Both are tracked in the run summary. If the bulk call as a whole fails (for example, it throws), every document in the batch is reported as failed, after any configured retries.

---

## Practical Guide

For how to configure indexers — generic parameters, field filtering, deletion mechanics, and backend-specific settings — see [Indexers]({{< relref "docs/ingest-design/indexers" >}}) in the Ingest Designer Guide.

For how to build a custom Indexer, see [Developing Indexers]({{< relref "docs/developer-guide/developing-indexers" >}}).

For raising indexing throughput — batch tuning, concurrent bulk sends (`indexer.maxConcurrentBatches`), scaling consumers vs. concurrent batches, and delivery semantics — see [Indexing Throughput]({{< relref "docs/operations/indexing-throughput" >}}).
