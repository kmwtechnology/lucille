---
title: Ingest Designer Guide
weight: 6
date: 2025-06-09
description: >
  Everything you need to design and configure Lucille ingests — from writing your first config to advanced patterns.
---

This guide is for anyone using Lucille to accomplish a search ingestion task. You'll be writing configuration files that define one or more ingests — choosing connectors, composing pipelines, configuring indexers, and tuning run parameters. No Java code is required.

For conceptual explanations of how these components work and why they are designed the way they are, see [Architecture]({{< relref "docs/architecture" >}}).

---

## Getting Started

- [Writing a Config]({{< relref "docs/reference/writing-a-config" >}}) — Anatomy of a Lucille config file: required elements, available settings, validation, and HOCON basics.
- [Defining Pipelines]({{< relref "docs/reference/defining-pipelines" >}}) — Pipeline syntax, connecting connectors to pipelines, multiple pipelines, conditions, and stage reuse patterns.
- [Control Flow]({{< relref "docs/reference/control-flow" >}}) — How to control what happens to a document as it moves through a pipeline: conditions, skipping, dropping, errors, child documents, and connector sequencing.

## Component Reference

- [Connectors]({{< relref "docs/reference/connectors" >}}) — Common parameters, sequencing, and the full catalogue of built-in connectors (File, Database, Kafka, RSS, Solr, Parquet).
- [Stages]({{< relref "docs/reference/stages" >}}) — Stage configuration, conditions reference, and the complete stage catalogue organized by category.
- [Indexers]({{< relref "docs/reference/indexers" >}}) — Generic indexer parameters, field filtering, deletion mechanics, and backend-specific configuration (Solr, OpenSearch, Elasticsearch, CSV, Pinecone, Weaviate).

## Cookbooks

- [File Ingestion]({{< relref "docs/reference/cookbooks/file_ingest_cookbook" >}}) — Ingest files from local, S3, Azure, or GCS. Covers CSV, JSON, XML, incremental mode, tombstones, and Tika text extraction.
- [Vector Search]({{< relref "docs/reference/cookbooks/vector_search_cookbook" >}}) — Build end-to-end vector search pipelines: chunk text, generate embeddings, and index into Pinecone or Weaviate.
- [RSS Ingestion]({{< relref "docs/reference/cookbooks/rss_cookbook" >}}) — Ingest RSS feeds into CSV or OpenSearch, including incremental mode.
