---
weight: 11
title: Indexer
date: 2025-06-09
description: An Indexer sends processed Documents to a specific destination.
---

## Indexers

An *Indexer* is a thread that retrieves processed Documents from the end of a Pipeline and sends them in batches to a specific destination. For users of Lucille, this destination will most commonly be a search engine.

Only one Indexer can be defined in a Lucille run. All pipelines feed to the same Indexer.

## Configuration Structure

Indexer configuration has two parts: the generic `indexer` block, and a backend-specific config block. For example, using the SolrIndexer:

```hocon
# Generic indexer config
indexer {
  type: "Solr"
  batchSize: 100
  blacklist: ["internal_field"]
}

# Solr-specific config
solr {
  useCloudClient: true
  url: ["http://localhost:8983/solr"]
  defaultCollection: "my-collection"
}
```

`indexer.type` is shorthand for a built-in indexer: `"Solr"`, `"OpenSearch"`, `"Elasticsearch"`, or `"CSV"`. For plugin indexers, use `indexer.class` with the fully qualified class name instead.

## Generic `indexer` Configuration Parameters

These parameters are available on all Indexers:

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

  # Optional: also delete all documents referencing the same source file
  deleteByFieldField: "delete_by_field"
  deleteByFieldValue: "delete_by_value"
}
```

For per-indexer configuration reference — Solr, OpenSearch, Elasticsearch, CSV, NopIndexer, and plugin indexers — see the [Indexers Reference]({{< relref "docs/reference/indexers" >}}).

## Indexer Batching

Documents accumulate in a batch and are flushed when either condition is met:
- The batch reaches `batchSize` documents (default: 100).
- `batchTimeout` milliseconds have elapsed since the last document was added or the last flush (default: 100ms).

The timeout flush ensures documents are not left waiting indefinitely in low-volume scenarios.

**Batch-level vs. per-document failures:** A bulk-API failure that rejects the entire request fails all documents in the batch. Individual document rejections in the response (e.g., mapping errors) fail only those specific documents — the rest succeed. Both cases are tracked separately in the run summary.
