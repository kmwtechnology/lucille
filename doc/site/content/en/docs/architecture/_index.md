---
title: Architecture
weight: 3
description: >
  Understanding Lucille's core components, topology, and design.
---

Lucille separates the three concerns of ETL — reading, transforming, and writing — into distinct components that run concurrently.

## Start Here

| Section | What It Covers |
|---|---|
| [Overview]({{< relref "overview" >}}) | The problem Lucille solves, the core architecture, and the document lifecycle |
| [Topology]({{< relref "topology" >}}) | Deployment modes from single-JVM to fully distributed |
| [Components]({{< relref "components" >}}) | Reference pages for each component (Document, Pipeline, Publisher, Connectors, Indexers, Stages, Config) |
| [Design Rationale]({{< relref "design-rationale" >}}) | The 24 requirements that explain why the system is designed the way it is |

## How Components Interact

The components communicate through queues. In local mode these are in-memory `LinkedBlockingQueue` instances. In distributed mode they are Kafka topics. The component code is identical in both cases — only the messenger implementation changes.

```
Connector → [processing queue] → Worker(s) → [indexing queue] → Indexer
                                       ↓ events ↑
                                  Publisher (run accounting)
```

For in-depth explanations of how each subsystem works internally, see [Internals]({{< relref "docs/internals" >}}).
