---
title: Overview
weight: 1
description: >
  A narrative introduction to Lucille's architecture — the problem it solves, the core design, and how documents flow through the system.
---

Read these pages in order for a complete introduction to Lucille's architectural design.

1. **[The Problem of Search ETL]({{< relref "problem" >}})** — Why a simple sequential ingest loop falls short in production and what needs to change.
2. **[Parallelizing Search ETL]({{< relref "parallelizing-search-etl" >}})** — How Lucille maps the three ETL functions to concurrent components communicating through queues, and how it tracks document lifecycle across an asynchronous system.
3. **[Pluggable Queueing and the Deployment Model]({{< relref "pluggable-queueing" >}})** — How the Messenger abstraction makes the queue implementation pluggable, enabling flexible deployment and straightforward testing.
4. **[Topology]({{< relref "topology" >}})** — Batch and streaming models, the WorkerIndexer, and deployment configurations from single-JVM to fully distributed.
5. **[Document Lifecycle]({{< relref "document-lifecycle" >}})** — The complete journey of a single Document through the system, plus cross-cutting concerns like logging, metrics, and testing.
6. **[Design Rationale]({{< relref "design-rationale" >}})** — The 24 requirements that govern Lucille's architecture and why the system is designed the way it is.
