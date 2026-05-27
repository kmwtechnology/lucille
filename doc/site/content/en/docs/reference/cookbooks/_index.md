---
title: Cookbooks
weight: 7
description: >
  Practical cookbooks for common Lucille pipeline patterns.
---

Lucille cookbooks are practical, step-by-step guides for common ingestion patterns.

## Available Cookbooks

- **[File Ingestion]({{< relref "docs/reference/cookbooks/file_ingest_cookbook" >}})** — Ingest files from the local filesystem, Amazon S3, Azure Blob Storage, and Google Cloud Storage. Covers CSV, JSON, XML, incremental mode, tombstone deletions, and Tika text extraction.

- **[Vector Search]({{< relref "docs/reference/cookbooks/vector_search_cookbook" >}})** — Build end-to-end vector search pipelines: chunk text, generate embeddings (OpenAI or local via JLama), and index into Pinecone or Weaviate.

- **[RSS Ingestion]({{< relref "docs/reference/cookbooks/rss_cookbook" >}})** — Ingest RSS feeds into CSV or OpenSearch, including incremental mode.
