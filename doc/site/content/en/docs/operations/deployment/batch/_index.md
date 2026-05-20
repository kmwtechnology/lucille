---
title: Batch
weight: 1
date: 2025-06-09
description: >
  Batch deployment modes — finite ingests with completion detection.
---

In batch mode, a Runner triggers a finite ingest: the Connector retrieves a bounded set of data, Workers process it, and the Indexer sends it to the search backend. The system detects when all work is complete and reports a run summary.

| Mode | When to Use | Command |
|---|---|---|
| **[Local]({{< relref "local" >}})** | Development, small jobs (< millions of docs) | `java -cp ... com.kmwllc.lucille.core.Runner` |
| **[Distributed]({{< relref "distributed" >}})** | Production scale-out with multiple workers | Separate Runner, Worker, and Indexer processes |
