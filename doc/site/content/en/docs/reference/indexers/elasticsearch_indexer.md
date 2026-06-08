---
title: Elasticsearch Indexer
weight: 3
date: 2025-06-09
description: Configuration reference for the Elasticsearch Indexer, including join field support.
---

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

## Join Field Support (Detailed)

Elasticsearch's join field allows parent-child relationships within a single index. The ElasticsearchIndexer supports this via the `elastic.join` config block:

| Parameter | Description |
|---|---|
| `joinFieldName` | The name of the join field in your Elasticsearch mapping. |
| `isChild` | Whether documents indexed by this indexer are children in the join relationship. |
| `childName` | The relation name for the child (must match your mapping). |
| `parentDocumentIdSource` | The document field that holds the parent's ID. Used to set the `_routing` parameter (required for joins). |

When `isChild: true`, the indexer adds a join field to each document with the child relation name and sets routing to the parent's ID. Elasticsearch requires parent and child documents to be on the same shard, so `indexer.routingField` should also be set to the same field as `parentDocumentIdSource`.

---

## Routing and Versioning

Same as OpenSearch: supports `indexer.routingField` for custom shard routing and `indexer.versionType` for optimistic concurrency control using Kafka offsets in distributed mode.

---

## Partial Update Mode

When `update: true`, documents are sent as partial updates (doc-as-upsert) rather than full index operations. Same behavior as the OpenSearch Indexer.

---

## Differences from OpenSearch Indexer

- **No retry support** — ElasticsearchIndexer does not wrap failures as `IndexerRetryableException`. The base class retry machinery will not trigger for Elasticsearch failures. If retries are needed, configure them at the Elasticsearch client or load balancer level.
- **No `indexOverrideField` support** — All documents are sent to the single configured `index`. You cannot route documents to different indices within the same batch.
- **Child documents** — The code iterates attached children but does not currently add them to the indexed document (this is a known TODO). Use emitted children (separate documents) instead of attached children if you need child documents indexed in Elasticsearch.

---

## Troubleshooting

**Join field errors:**
Ensure your Elasticsearch index mapping includes the join field with the correct parent and child relation names. The `joinFieldName` in config must match the mapping exactly.

**Routing errors with joins:**
Parent and child documents must be on the same shard. Set `indexer.routingField` to the field containing the parent ID.

**"index not found":**
The target index must exist before indexing. Create it manually or via an index template.
