---
title: Streaming
weight: 2
date: 2025-06-09
description: >
  Streaming deployment modes — continuous ingestion with no run boundary.
---

In streaming mode, Lucille runs without a Runner or Connector. An external system places documents onto a Kafka source topic, and Worker or WorkerIndexer processes consume and process them continuously — with no run boundary or completion accounting.

| Mode | When to Use | Command |
|---|---|---|
| **[Distributed]({{< relref "distributed" >}})** | Separate Worker and Indexer processes for maximum flexibility | `Worker` + `Indexer` processes |
| **[Hybrid]({{< relref "hybrid" >}})** | Co-located processing and indexing in fewer processes | `WorkerIndexer` processes |
