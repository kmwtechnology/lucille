---
title: Indexers
weight: 3
date: 2025-06-09
description: Configuration reference for built-in and plugin indexers shipped with Lucille.
---

For conceptual documentation — what an Indexer is, batching semantics, field filtering, deletion mechanics, and generic configuration parameters — see [Architecture: Indexers]({{< relref "docs/architecture/components/Indexers" >}}).

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

The following indexers require an additional Maven dependency.

---

### Pinecone Indexer

`com.kmwllc.lucille.pinecone.indexer.PineconeIndexer`

Indexes vector embeddings into [Pinecone](https://www.pinecone.io/). Config block: `pinecone { ... }`

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-pinecone</artifactId>
  <version>${lucille.version}</version>
</dependency>
```

| Parameter | Type | Required | Description |
|---|---|---|---|
| `apiKey` | String | Yes | Pinecone API key. Use `${PINECONE_API_KEY}` for environment variable substitution. |
| `index` | String | Yes | Name of the Pinecone index to write to. |
| `vectorField` | String | Yes | Document field containing the vector embedding. |
| `namespace` | String | No | Pinecone namespace. Default: `"default"`. |

```hocon
indexer {
  class: "com.kmwllc.lucille.pinecone.indexer.PineconeIndexer"
  deletionMarkerField: "is_deleted"
  deletionMarkerFieldValue: "true"
}

pinecone {
  apiKey: ${PINECONE_API_KEY}
  index: "my-index"
  vectorField: "content_vector"
  namespace: "default"
}
```

---

### Weaviate Indexer

`com.kmwllc.lucille.weaviate.indexer.WeaviateIndexer`

Indexes documents into [Weaviate](https://weaviate.io/). Config block: `weaviate { ... }`

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-weaviate</artifactId>
  <version>${lucille.version}</version>
</dependency>
```
