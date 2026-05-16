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

## Lucille Indexers (Core)

### Solr Indexer

`com.kmwllc.lucille.indexer.SolrIndexer`

Config block: `solr { ... }`

| Parameter | Type | Required | Description |
|---|---|---|---|
| `url` | List\<String\> | No* | Solr base URLs (e.g., `http://localhost:8983/solr`). |
| `useCloudClient` | Boolean | No | Use SolrCloud client. Default: `false`. |
| `defaultCollection` | String | No | Target Solr collection. |
| `zkHosts` | List\<String\> | No* | ZooKeeper addresses for SolrCloud (e.g., `["zk1:2181", "zk2:2181"]`). |
| `zkChroot` | String | No | ZooKeeper chroot path for SolrCloud (e.g., `"/solr"`). |
| `userName` | String | No | HTTP Basic Auth username. |
| `password` | String | No | HTTP Basic Auth password. |
| `acceptInvalidCert` | Boolean | No | Accept invalid TLS certificates. Default: `false`. |

*Provide either `url` or (for SolrCloud) `zkHosts`.

```hocon
# Single node
solr { url: "http://localhost:8983/solr/my-collection" }

# SolrCloud with URL
solr {
  useCloudClient: true
  url: ["http://localhost:8983/solr"]
  defaultCollection: "my-collection"
}

# SolrCloud with ZooKeeper
solr {
  useCloudClient: true
  zkHosts: ["zk1:2181", "zk2:2181"]
  zkChroot: "/solr"
  defaultCollection: "my-collection"
}
```

---

### OpenSearch Indexer

`com.kmwllc.lucille.indexer.OpenSearchIndexer`

Config block: `opensearch { ... }`

| Parameter | Type | Required | Description |
|---|---|---|---|
| `url` | String | Yes | OpenSearch endpoint URL including credentials if needed (e.g., `https://admin:password@localhost:9200`). |
| `index` | String | Yes | Target index name. |
| `update` | Boolean | No | Use the partial update API instead of index (upsert). Default: `false`. |
| `acceptInvalidCert` | Boolean | No | Accept invalid TLS certificates. Default: `false`. |

Also supports `indexer.routingField` and `indexer.versionType` (via the generic indexer block).

```hocon
opensearch {
  url: "https://admin:admin@localhost:9200"
  url: ${?OPENSEARCH_URL}
  index: "my-index"
  acceptInvalidCert: true
}
```

---

### Elasticsearch Indexer

`com.kmwllc.lucille.indexer.ElasticsearchIndexer`

Config block: `elastic { ... }`

Supports all OpenSearch parameters, plus parent-child join support:

| Parameter | Type | Required | Description |
|---|---|---|---|
| `url` | String | Yes | Elasticsearch endpoint URL. |
| `index` | String | Yes | Target index name. |
| `update` | Boolean | No | Use partial update API. Default: `false`. |
| `acceptInvalidCert` | Boolean | No | Accept invalid TLS certs. Default: `false`. |
| `parentName` | String | No | Parent relation name for join field mappings. |

**Join field support** (for parent-child mappings):

```hocon
elastic {
  url: "http://localhost:9200"
  index: "my-index"
  join: {
    joinFieldName: "my_join_field"
    isChild: true
    childName: "my_child"
    parentDocumentIdSource: "parent_id_field"
  }
}
indexer {
  type: "Elasticsearch"
  routingField: "routing_field"
}
```

---

### CSV Indexer

`com.kmwllc.lucille.indexer.CSVIndexer`

Config block: `csv { ... }`

| Parameter | Type | Required | Description |
|---|---|---|---|
| `path` | String | Yes | Output CSV file path. |
| `columns` | List\<String\> | Yes | Ordered list of document fields to write as columns. |
| `includeHeader` | Boolean | No | Write a header row. Default: `true`. |
| `append` | Boolean | No | Append to an existing file. Default: `false`. |

```hocon
indexer { type: "CSV" }
csv {
  path: "./output.csv"
  columns: ["id", "title", "body", "published_at"]
}
```

**Limitations:** CSVIndexer does not support `indexer.indexOverrideField`.

---

### NopIndexer (No-op)

`com.kmwllc.lucille.indexer.NopIndexer`

Discards all documents without sending them anywhere. Useful for testing pipelines when indexing output is not needed. Same as setting `indexer.sendEnabled: false`.

## Lucille Indexers (Plugins)

| Indexer | Plugin | Config block |
|---|---|---|
| `PineconeIndexer` | lucille-pinecone | `pinecone { ... }` |
| `WeaviateIndexer` | lucille-weaviate | `weaviate { ... }` |

See [Plugins]({{< relref "docs/architecture/components/plugins" >}}) for setup and configuration.

## Indexer Batching

Documents accumulate in a batch and are flushed when either condition is met:
- The batch reaches `batchSize` documents (default: 100).
- `batchTimeout` milliseconds have elapsed since the last document was added or the last flush (default: 100ms).

The timeout flush ensures documents are not left waiting indefinitely in low-volume scenarios.

**Batch-level vs. per-document failures:** A bulk-API failure that rejects the entire request fails all documents in the batch. Individual document rejections in the response (e.g., mapping errors) fail only those specific documents — the rest succeed. Both cases are tracked separately in the run summary.
