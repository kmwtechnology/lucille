---
title: Indexers
weight: 5
date: 2025-06-09
description: Configuration reference for built-in and plugin indexers shipped with Lucille.
---

For conceptual documentation — what an Indexer is, why batching matters, deletion as a design pattern, and error handling at the batch level — see [Architecture: Indexer]({{< relref "docs/architecture/components/Indexers" >}}).

## Generic `indexer` Configuration

Indexer configuration has two parts: the generic `indexer` block (common to all backends), and a backend-specific config block (e.g., `solr`, `opensearch`, `elastic`, `csv`).

```hocon
indexer {
  type: "Solr"
  batchSize: 100
  batchTimeout: 100
  blacklist: ["internal_field"]
}

solr {
  url: "http://localhost:8983/solr/my-collection"
}
```

`indexer.type` is shorthand for a built-in indexer: `"Solr"`, `"OpenSearch"`, `"Elasticsearch"`, or `"CSV"`. For plugin indexers, use `indexer.class` with the fully qualified class name instead.

### Generic Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `type` | String | — | Shorthand for built-in indexers: `Solr`, `OpenSearch`, `Elasticsearch`, `CSV`. |
| `class` | String | — | Fully qualified class name for plugin or custom indexers. |
| `batchSize` | Integer | 100 | Number of documents to accumulate before sending a batch. |
| `batchTimeout` | Integer (ms) | 100 | Milliseconds since last add or flush before the batch is sent regardless of size. |
| `idOverrideField` | String | — | Document field whose value is used as the ID sent to the destination (instead of `id`). |
| `indexOverrideField` | String | — | Document field whose value determines the target index/collection. Triggers per-index batching. Not supported by OpenSearch or Elasticsearch indexers. |
| `whitelist` | List\<String\> | — | Only these fields are sent to the destination. Fields on the blacklist are still excluded. |
| `blacklist` | List\<String\> | — | These fields are never sent to the destination. |
| `sendEnabled` | Boolean | true | Set to `false` to disable actual indexing (useful for testing or pipeline validation). |
| `deletionMarkerField` | String | — | Field name that marks a document as a deletion request. |
| `deletionMarkerFieldValue` | String | — | Value in `deletionMarkerField` that triggers a deletion. Both must be set together. |
| `deleteByFieldField` | String | — | Field name containing the index field to use in a delete-by-query operation. |
| `deleteByFieldValue` | String | — | Field name containing the value to match in a delete-by-query operation. Both must be set together. |

### Field Filtering

Whitelist and blacklist are applied **at indexing time**, not during pipeline processing. Stages see all fields on a document; only the Indexer strips fields before sending to the backend. Reserved internal fields (`___dropped`, `___skipped`, `___children`) are always stripped.

When `indexOverrideField` is set, the Indexer uses a `MultiBatch` — maintaining a separate batch per distinct field value and flushing each independently when it reaches `batchSize` or `batchTimeout`.

### Deletion Mechanics

Two distinct deletion mechanisms are available:

**Delete by ID:** Set `deletionMarkerField` and `deletionMarkerFieldValue`. When a document has the marker field set to the marker value, the Indexer issues a delete-by-ID against the search backend for that document's ID.

**Delete by query:** Also set `deleteByFieldField` and `deleteByFieldValue`. When a document has all four deletion fields set, the Indexer issues a delete-by-query: it deletes all documents in the index where `deleteByFieldField`'s referenced field equals the value in `deleteByFieldValue`'s referenced field.

```hocon
indexer {
  type: "Solr"
  deletionMarkerField: "file_expired"
  deletionMarkerFieldValue: "true"
  deleteByFieldField: "delete_by_field"
  deleteByFieldValue: "delete_by_value"
}
```

### Batching Behavior

Documents accumulate in a batch and are flushed when either condition is met:
- The batch reaches `batchSize` documents (default: 100).
- `batchTimeout` milliseconds have elapsed since the last document was added or the last flush (default: 100ms).

The timeout flush ensures documents are not left waiting indefinitely in low-volume scenarios.

**Batch-level vs. per-document failures:** A bulk-API failure that rejects the entire request fails all documents in the batch. Individual document rejections in the response (e.g., mapping errors) fail only those specific documents — the rest succeed. Both cases are tracked separately in the run summary.

---

## Indexer Catalogue

### Core Indexers

- [Solr Indexer]({{< relref "docs/reference/indexers/solr_indexer" >}}) — Single-node and SolrCloud.
- [OpenSearch Indexer]({{< relref "docs/reference/indexers/opensearch_indexer" >}}) — OpenSearch with optional partial updates.
- [Elasticsearch Indexer]({{< relref "docs/reference/indexers/elasticsearch_indexer" >}}) — Elasticsearch with join field support.
- [CSV Indexer]({{< relref "docs/reference/indexers/csv_indexer" >}}) — Write pipeline output to a CSV file.
- [NopIndexer]({{< relref "docs/reference/indexers/nop_indexer" >}}) — No-op indexer for testing.

### Plugin Indexers

- [Pinecone Indexer]({{< relref "docs/reference/indexers/pinecone_indexer" >}}) — Index vector embeddings into Pinecone.
- [Weaviate Indexer]({{< relref "docs/reference/indexers/weaviate_indexer" >}}) — Index documents and vectors into Weaviate.
