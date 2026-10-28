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
| `connectTimeoutMs` | Long | No | Maximum time to establish a connection, in ms. Default: `5000`. |
| `socketTimeoutMs` | Long | No | Maximum time to wait for data on an open connection, including a response, in ms. Applies to delete-by-query too, so raise it above the slowest expected request, or set `-1` for no limit. Default: `30000`. |
| `connectionTimeToLiveMs` | Long | No | Maximum lifetime of a pooled connection, in ms, so long-running clients behind a load balancer or Kubernetes Service reconnect and reach new nodes. Lower it to rebalance faster. Default: `300000` (5 minutes). |
| `versionConflictIsSuccess` | Boolean | No | Treat a 409 version conflict on an upload or a versioned delete by id as success. Requires `indexer.versionType` `external` or `external_gte`. See [Version Conflicts](#version-conflicts). Default: `false`. |
| `splitOnRequestTooLarge` | Boolean | No | Split a bulk upload that OpenSearch rejects with HTTP 413 in half, and send each half on its own. See [Request Too Large](#request-too-large). Default: `false`. |

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

By default this uses the Kafka offset (requiring distributed mode with `KafkaDocument` instances). To use a version value from the document itself — useful in local mode or when the version is sourced from an upstream system — set `indexer.versionField`:

```hocon
indexer {
  versionType: "external_gte"
  versionField: "my_version_field"
}
```

| Parameter | Type | Required | Description |
|---|---|---|---|
| `indexer.versionType` | String | No | `external` or `external_gte`. Enables optimistic concurrency control. |
| `indexer.versionField` | String | No | Document field containing a numeric version value. When set, this field's value is used instead of the Kafka offset. Requires `versionType` to also be set. |

If `versionField` is set but the field is absent on a particular document, and the document is not a `KafkaDocument`, the version is omitted and the document is indexed without optimistic concurrency control.

### Versioned Deletes

A delete by id (a document marked with `indexer.deletionMarkerField`) is versioned too, when `versionType` is `external` or `external_gte` and the delete document has `versionField`. OpenSearch rejects the delete with a 409 if the index holds a newer version (or, for `external`, the same version), and keeps a tombstone at the delete's version that rejects an older upload arriving after it, for as long as the index keeps tombstones (`index.gc_deletes`, 60 seconds by default). Without this, an older delete could remove a newer document, and a late upload could bring back a deleted one.

A delete document without `versionField` is sent unversioned, as before. Deletes never use the Kafka offset as their version, because an offset is not comparable to a version taken from `versionField`. Delete-by-query is never versioned.

### Version Conflicts

With external versioning, OpenSearch rejects an operation with a 409 when the index already holds a version at least as new as the operation's: the operation was superseded by a newer write. By default a 409 is a failure: an upload's 409 fails that document, and a delete's 409 fails the batch, as any delete error does.

Set `opensearch.versionConflictIsSuccess: true` to treat a 409 as success instead, for uploads and versioned deletes by id alike. This suits a pipeline that may send the same or an older version again (for example, when reprocessing), where a superseded write isn't an error. Setting it without `indexer.versionType` `external` or `external_gte` is a configuration error, because without external versioning a 409 doesn't mean the index is already up to date.

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

When `indexer.maxRetries` is set, a retry of a batch is aware of what the earlier attempts did:

- A document OpenSearch already accepted is not sent again, and counts as succeeded. With `external_gte`, re-sending an accepted document is a real write, and once a newer delete's tombstone has expired (`index.gc_deletes`) it can re-create the deleted document.
- A document that failed with a non-retryable error is not sent again, and its failure is reported again.
- Once any upload in the batch has been accepted, an exception from a later request (the uploads, the delete by id, or the delete by query) fails the documents in that request, not the whole batch. A retryable exception still triggers a retry.

Deletes are sent again on each attempt. Without `indexer.maxRetries`, every attempt sends the whole batch and an exception fails the whole batch, as before.

A document marked for delete by query whose `deleteByFieldField` or `deleteByFieldValue` field is null fails on its own; it can't be part of a terms query.

### Request Too Large

OpenSearch rejects a request body over `http.max_content_length` (100MB by default) with HTTP 413. `indexer.batchByteSize` keeps bulk requests below a size, but it is an estimate. By default, a 413 fails the whole batch; it arrives without a parseable error body, so it is reported as a transport failure without a status code, and it is retried with the `-1` status code when that is in `indexer.retryableStatusCodes`.

With `opensearch.splitOnRequestTooLarge: true`, a 413 on a bulk upload is recognized from the response, and the batch is split in half and each half sent on its own, recursively, until each part is accepted. A single document that still gets a 413 fails permanently by itself and is not retried. Once a batch is split, each half's failures stay with that half's documents.

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
