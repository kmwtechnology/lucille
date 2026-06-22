---
title: Getting Started
weight: 2
description: >
  What Lucille is, how it works, and how to run your first pipeline.
aliases:
  - /docs/about/
---

Lucille is a production-grade Search ETL framework for loading data into Lucene-based search engines — Apache Solr, Elasticsearch, and OpenSearch — and vector databases such as Pinecone and Weaviate. It supports batch, incremental, and streaming ingestion with inline document enrichment, and runs as a single JAR with no persistent server to manage. Lucille is Java-based and open-source, developed and maintained by [KMW Technology](https://kmwllc.com/).

## How Lucille Works

1. A **Connector** retrieves data from a source system and publishes it as Lucille Documents.
2. **Workers** route each Document through a configurable enrichment **Pipeline** built from composable **Stages**.
3. An **Indexer** sends the processed Documents to the search backend.
4. These three components communicate through a messaging layer — either in-memory queues (local mode, single JVM) or Apache Kafka (distributed mode, multiple JVMs at scale).
5. Document lifecycle events — publication, processing, indexing, failure, and drops — are tracked so Lucille knows when a run is complete and can report exact success and failure counts.

For a deeper look at the architecture, see [Architecture Overview]({{< relref "docs/architecture/overview" >}}).
