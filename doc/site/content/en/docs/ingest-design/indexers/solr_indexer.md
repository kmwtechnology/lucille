---
title: Solr Indexer
weight: 1
date: 2025-06-09
description: Configuration reference for the Solr Indexer — single-node and SolrCloud.
---

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

## Child Document Support

SolrIndexer is the only Lucille indexer that fully supports attached child documents. When a document has children (via `doc.addChild()`), they are automatically converted to Solr's `_childDocuments_` structure. The `___children` field itself is never sent to Solr — only the nested Solr child documents are added.

Children respect the configured blacklist and whitelist: filtered fields are removed from child documents before they are added to the Solr parent.

---

## Collection Routing

When `indexer.indexOverrideField` is set, documents are routed to different Solr collections within the same batch. The value of the override field on each document determines which collection receives it. The override field itself is stripped from the document before sending.

The target collection must already exist in Solr — the indexer does not create collections.

---

## Interleaved Add/Delete Ordering

SolrIndexer preserves the order of adds and deletes within a batch. If a batch contains an add followed by a delete for the same document ID (or vice versa), the operations are sent to Solr in the correct sequence. This is important for incremental ingestion where a document may be re-added and then deleted (or deleted and re-added) within the same run.

Internally, the indexer detects when an add and a delete for the same ID appear in the same batch and flushes the earlier operation before queuing the later one.

---

## Delete-by-Query via Terms Queries

When `deleteByFieldField` and `deleteByFieldValue` are configured, SolrIndexer constructs a Solr terms query for efficient bulk deletion rather than issuing individual delete-by-query calls. Multiple deletions targeting the same field are combined into a single terms query:

```
(+{!terms f='category' v='obsolete,deprecated'})
```

This reduces the number of round-trips to Solr when many documents are marked for deletion in the same batch.

---

## Limitations

- **Object fields not supported** — If a document field contains a nested Map or Object, SolrIndexer throws an `IndexerException`. Flatten nested structures in your pipeline before indexing to Solr.

---

## Troubleshooting

**Connection validation fails:**
- **Single-node mode** (`useCloudClient: false`): Validation uses `solrClient.ping()`. If this fails, check that the URL includes the collection path (e.g., `http://localhost:8983/solr/my-collection`) and that the collection exists.
- **SolrCloud mode** (`useCloudClient: true`): Validation checks cluster status via the Collections API. If this fails, the cluster is unreachable — check ZooKeeper connectivity and that at least one Solr node is running.

**"Object field is not supported by the SolrIndexer":**
A document field contains a nested JSON object. Add a stage to your pipeline that flattens or removes the field before it reaches the indexer.
