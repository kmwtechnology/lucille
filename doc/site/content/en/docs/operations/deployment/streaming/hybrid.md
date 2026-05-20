---
title: Hybrid
weight: 2
date: 2025-06-09
description: Running WorkerIndexer processes for streaming ingestion with co-located processing and indexing.
---

## Hybrid Streaming Mode

In hybrid streaming mode, Lucille runs one or more `WorkerIndexer` processes that co-locate document processing and indexing in a single JVM. Like distributed streaming, there is no Runner or Connector — an external system places documents onto a Kafka source topic, and the WorkerIndexer processes consume, enrich, and index them continuously.

**When to use:** Streaming ingestion where you want the horizontal scalability of distributed mode but with simpler operations — fewer processes to manage, no separate Indexer deployment, and reduced network hops between processing and indexing.

{{% pageinfo %}}
This page is under construction. See [Distributed]({{< relref "distributed" >}}) for WorkerIndexer usage details in the meantime.
{{% /pageinfo %}}
