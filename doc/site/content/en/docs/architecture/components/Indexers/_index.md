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

A batch is flushed when either of two conditions is met: the batch reaches a configured size, or a timeout expires since the last flush. The timeout ensures documents are not left waiting indefinitely in low-volume scenarios.

## Single Indexer Per Run

Only one Indexer can be defined in a Lucille run. All pipelines feed to the same Indexer. This simplifies the system — there is one destination, one set of batching parameters, one connection to manage — and reflects the common reality that a search ingestion project writes to a single search backend.

When documents from different pipelines need to land in different indices within the same backend, Lucille supports index routing: a field on the document determines which index it is sent to, without requiring multiple Indexer definitions.

## Deletion Support

Indexers support two deletion mechanisms — delete-by-ID and delete-by-query — triggered by marker fields on the document. This enables CDC patterns and incremental ingestion: a Connector can emit a document that represents "delete this record from the index" rather than "index this record," and the Indexer translates that intent into the appropriate backend operation.

## Field Filtering

The Indexer applies field filtering at the boundary — stripping internal fields, applying whitelist/blacklist rules — so that only the intended fields reach the search backend. This filtering happens at indexing time, not during pipeline processing, so Stages always see the full document.

## Error Handling at the Batch Level

Search engine bulk APIs can return mixed results: some documents accepted, others rejected. The Indexer inspects per-document responses and reports individual failures without failing the entire batch. Documents that succeed are marked complete; documents that fail are marked failed. Both are tracked in the run summary.

---

## Practical Guide

For how to configure indexers — generic parameters, field filtering, deletion mechanics, and backend-specific settings — see [Indexers]({{< relref "docs/reference/indexers" >}}) in the Ingest Designer Guide.

For how to build a custom Indexer, see [Developing New Components]({{< relref "docs/developer-guide/dev_new_components" >}}).
