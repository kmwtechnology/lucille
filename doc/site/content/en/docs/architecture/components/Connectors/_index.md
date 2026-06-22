---
weight: 3
title: Connector
date: 2024-10-15
description: A component that retrieves data from a source system and packages the data into Documents in preparation for transformation.
---

## What a Connector Does

A Connector is the component responsible for acquiring data from a source system and introducing it into Lucille as Documents. It is the entry point for all data in the system.

A Connector reads from its source — a database, a filesystem, a Kafka topic, an RSS feed, a search engine — and emits Documents one at a time by calling `publisher.publish(doc)`. It does not know how many Workers will process those documents, how long enrichment will take, or where the documents will ultimately be indexed. Its only job is to produce Documents and hand them off.

## Lifecycle

Every Connector goes through four lifecycle phases on each run:

1. **`preExecute(runId)`** — Called before `execute`. Use for setup: acquiring locks, creating temporary tables, validating source accessibility.
2. **`execute(publisher)`** — The main phase. Read from the source and call `publisher.publish(doc)` for each Document.
3. **`postExecute(runId)`** — Called only if `execute` succeeds. Use for cleanup: releasing locks, writing completion markers.
4. **`close()`** — Always called, even on failure. Use for releasing resources.

This lifecycle is enforced by the framework. The separation of `preExecute` from `execute` allows setup that should not be repeated on retry. The guarantee that `close()` is always called — regardless of whether `execute` or `postExecute` threw — ensures resources are never leaked.

## Sequential Execution

When multiple Connectors are defined in a single run, they execute in sequence. Each Connector runs to completion — all its documents processed and indexed — before the next begins. This ordering guarantee is enforced automatically by the Publisher's accounting system, without external orchestration.

This enables patterns like indexing parent documents before child documents that reference them by ID, or running a full ingest followed by a deletion pass.

## Decoupling from Downstream

A Connector is fully decoupled from the rest of the system. It does not know:
- How many Worker threads or processes will consume its output
- What pipeline will be applied to its documents
- Which search backend the documents will reach
- Whether the system is running in local or distributed mode

This decoupling is what allows the same Connector implementation to work identically in all deployment modes. The Connector publishes to a queue; everything downstream is the framework's concern.

---

## Practical Guide

For how to configure connectors — common parameters, config syntax, and the full catalogue of built-in connectors — see [Connectors]({{< relref "docs/reference/connectors" >}}) in the Ingest Designer Guide.

For how to build a custom Connector, see [Developing Connectors]({{< relref "docs/developer-guide/developing-connectors" >}}).
