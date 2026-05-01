---
title: Architecture
weight: 3
description: >
  Understanding Lucille's core components & topology.
---

Lucille separates the three concerns of ETL — reading, transforming, and writing — into distinct components that run concurrently.

## Core Components

| Component | Description |
|---|---|
| **[Connector]({{< relref "docs/architecture/components/Connectors" >}})** | Reads from a source system and emits Documents. |
| **[Pipeline]({{< relref "docs/architecture/components/pipeline" >}})** | An ordered sequence of Stages that transform Documents. |
| **[Worker]({{< relref "docs/architecture/components/worker" >}})** | Pulls Documents from a queue, runs them through a Pipeline, and pushes results onward. |
| **[Indexer]({{< relref "docs/architecture/components/Indexers" >}})** | Pulls processed Documents and writes them to the destination in batches. |
| **[Publisher]({{< relref "docs/architecture/components/publisher" >}})** | Tracks every Document from publication through terminal state. |
| **[Runner]({{< relref "docs/architecture/components/run" >}})** | Orchestrates a complete Lucille run end-to-end. |
| **[Document]({{< relref "docs/architecture/components/document" >}})** | The basic unit of data flowing through Lucille. |

## How Components Interact

The components communicate through queues. In local mode these are in-memory `LinkedBlockingQueue` instances. In distributed mode they are Kafka topics. The component code is identical in both cases — only the messenger implementation changes.

```
Connector → [source queue] → Worker(s) → [destination queue] → Indexer
                                   ↓ events ↑
                              Publisher (run accounting)
```

## Topology

Lucille supports four deployment topologies, from a single JVM to a fully distributed Kafka-based deployment. See [Topology]({{< relref "docs/architecture/topology" >}}) for details.

## Configuration

All components are configured via a single HOCON file. See [Config]({{< relref "docs/architecture/components/Config" >}}) for the full schema.
