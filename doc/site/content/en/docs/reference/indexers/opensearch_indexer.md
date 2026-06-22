---
title: OpenSearch Indexer
weight: 2
date: 2025-06-09
description: Configuration reference for the OpenSearch Indexer.
---

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

## Routing

When `indexer.routingField` is set, the value of that field on each document is used as the `_routing` parameter in the bulk request. This controls which shard receives the document. Use this when your OpenSearch index has custom shard routing configured.

---

## Version Type

When `indexer.versionType` is set to `external` or `external_gte`, the Kafka message offset is used as the document's version number. This enables optimistic concurrency control in streaming mode — OpenSearch will reject a write if the incoming version is not greater than (or equal to, for `external_gte`) the existing version.

This only works with documents that carry Kafka metadata (i.e., in distributed mode where documents are `KafkaDocument` instances).

---

## Partial Update Mode

When `update: true`, documents are sent as partial updates (doc-as-upsert) rather than full index operations:

- **Full index** (default): Replaces the entire document in the index. Fields not present in the new document are removed.
- **Partial update**: Merges fields into the existing document. Fields not present in the update are left unchanged.

```hocon
opensearch {
  url: ${OPENSEARCH_URL}
  index: "my-index"
  update: true
}
```

---

## Index Override

OpenSearch supports `indexer.indexOverrideField` for routing documents to different indices within the same batch. The value of the override field on each document determines the target index. The override field itself is stripped from the document before sending.

---

## Retry Behavior

OpenSearchIndexer wraps both transport-level failures (connection refused, timeout) and HTTP-level failures (e.g., 429, 503) as `IndexerRetryableException`. This enables the base class retry machinery when `indexer.maxRetries` is configured.

Per-document failures in the bulk response are also wrapped with the item's HTTP status code, allowing the retry logic to distinguish between retryable failures (e.g., 429 Too Many Requests) and permanent failures (e.g., 400 Bad Request).

---

## Child Documents

Attached children are flattened into the parent document as nested objects. They are not indexed as separate OpenSearch documents. This is different from Solr's `_childDocuments_` approach — in OpenSearch, children become part of the parent's JSON structure.

---

## Troubleshooting

**"index not found":**
The target index must exist before indexing. Create it manually or via an index template.

**TLS certificate errors:**
Set `acceptInvalidCert: true` for development environments with self-signed certificates. Do not use this in production.

**Authentication:**
Include credentials in the URL: `https://user:password@host:9200`. Use environment variable substitution to avoid hardcoding: `url: ${OPENSEARCH_URL}`.
