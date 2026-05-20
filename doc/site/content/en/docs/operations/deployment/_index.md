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
| **[Batch > Local]({{< relref "batch/local" >}})** | Development, small jobs (< millions of docs) | `java -cp ... com.kmwllc.lucille.core.Runner` |
| **[Batch > Distributed]({{< relref "batch/distributed" >}})** | Production scale-out with multiple workers | Separate Runner, Worker, and Indexer processes |
| **[Streaming > Distributed]({{< relref "streaming/distributed" >}})** | Continuous ingestion without a Runner | Separate Worker and Indexer processes |
| **[Streaming > Hybrid]({{< relref "streaming/hybrid" >}})** | Streaming with co-located processing and indexing | WorkerIndexer processes |

| Deployment Pattern | Details |
|---|---|
| [Docker Compose]({{< relref "docker-compose" >}}) | Quick distributed setup with all components in containers |
| [Kubernetes]({{< relref "kubernetes" >}}) | Production at scale with CronJobs, Deployments, and HPA |
| [Production Operations]({{< relref "production-operations" >}}) | Memory sizing, backpressure, graceful shutdown, monitoring |
