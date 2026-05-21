---
title: Deployment
weight: 2
date: 2025-06-09
description: >
  How to run Lucille in batch, streaming, and hybrid environments.
---

Choose a deployment mode based on your scale requirements. The same pipeline configuration runs in all modes — switching modes requires only a command-line flag change, not a code change.

| Mode | When to Use | Command |
|---|---|---|
| **[Local Batch]({{< relref "local-batch" >}})** | Development, small jobs (< millions of docs) | `java -cp ... com.kmwllc.lucille.core.Runner` |
| **[Distributed Batch]({{< relref "distributed-batch" >}})** | Production scale-out with multiple workers | Separate Runner, Worker, and Indexer processes |
| **[Distributed Streaming]({{< relref "distributed-streaming" >}})** | Continuous ingestion without a Runner | Separate Worker and Indexer processes |
| **[Hybrid Streaming]({{< relref "hybrid-streaming" >}})** | Streaming with co-located processing and indexing | WorkerIndexer processes |

| Deployment Pattern | Details |
|---|---|
| [Docker Compose]({{< relref "docker-compose" >}}) | Quick distributed setup with all components in containers |
| [Kubernetes]({{< relref "kubernetes" >}}) | Production at scale with CronJobs, Deployments, and HPA |
| [Production Operations]({{< relref "production-operations" >}}) | Memory sizing, backpressure, graceful shutdown, monitoring |
