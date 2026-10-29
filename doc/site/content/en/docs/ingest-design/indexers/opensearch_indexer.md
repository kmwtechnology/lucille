---
title: OpenSearch Indexer
weight: 2
date: 2025-06-09
description: Configuration reference for the OpenSearch Indexer.
---

`com.kmwllc.lucille.indexer.OpenSearchIndexer`

Config block: `opensearch { ... }`. Select it with `indexer.type: "OpenSearch"`.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `url` | String or List\<String\> | Yes | OpenSearch endpoint URL, or a list of endpoints for the same cluster; with a list, requests are spread across the hosts. Credentials in the URL (e.g., `https://admin:password@localhost:9200`) are used for basic auth for that host. Only the scheme, host, and port are used; a path in the URL is ignored. |
| `index` | String | Yes | Target index name. |
| `update` | Boolean | No | Send partial update operations instead of full index operations. Default: `false`. |
| `acceptInvalidCert` | Boolean | No | Accept invalid TLS certificates and skip hostname verification. Default: `false`. |
| `useCompression` | Boolean | No | Enable request compression in the HTTP client. Default: `false`. |
| `childDocumentsField` | String | No | Field in the indexed document that holds attached child documents. If not set, attached children are not indexed. |
| `connectTimeoutMs` | Long | No | Maximum time to establish a connection, including the TLS handshake, in ms. Must be at least 1. Default: `5000`. |
| `socketTimeoutMs` | Long | No | Maximum time to wait for data on an open connection, including a response, in ms. Applies to delete-by-query too, so raise it above the slowest expected request, or set `-1` for no limit. Any other value below 1 fails at startup. A timeout is a transport failure (status `-1`), retried when `indexer.maxRetries` is set; OpenSearch may still complete the request. Default: `30000`. |
| `connectionTimeToLiveMs` | Long | No | Maximum lifetime of a pooled connection, in ms, so long-running clients behind a load balancer or Kubernetes Service reconnect and reach new nodes. Lower it to rebalance faster. Expiry is checked when a connection is next used, so it never interrupts a request. Must be at least 1. Default: `300000` (5 minutes). |
| `maxConnectionsPerRoute` | Integer | No | Maximum pooled connections to each host. Must be at least 1. Default: the larger of `5` and `indexer.maxConcurrentBatches` + 1. |
| `maxConnectionsTotal` | Integer | No | Maximum pooled connections across all hosts. Must be at least 1. Default: the larger of `25` and `maxConnectionsPerRoute`. |

Also reads `indexer.routingField`, `indexer.versionType`, `indexer.versionField`, and `indexer.indexOverrideField` from the generic indexer block.

```hocon
opensearch {
  url: "https://admin:admin@localhost:9200"
  url: ${?OPENSEARCH_URL}
  index: "my-index"
  acceptInvalidCert: true
}
```

An invalid connection setting (for example a timeout or connection limit below 1) fails at startup when the client is built. The startup connection check only pings the cluster; it does not check that `index` exists. With `indexer.sendEnabled: false`, no client is built and these checks are skipped.

The indexer supports `indexer.maxConcurrentBatches` greater than 1. All sends share one client, and the connection pool defaults above grow with the concurrency so sends do not wait for a connection. If you set `maxConnectionsPerRoute` yourself, keep it above `indexer.maxConcurrentBatches`.

---

## Routing

When `indexer.routingField` is set, the value of that field on each document (first value, if multi-valued) is used as the `_routing` parameter in the bulk request. A document without the field is sent without routing. Routing is not sent with deletes by ID or delete-by-query. This controls which shard receives the document. Use this when your OpenSearch index has custom shard routing configured.

---

## Version Type

`indexer.versionType` is an OpenSearch `VersionType`, as the constant name (`External`, `ExternalGte`, `Internal`, `Force`) or the JSON spelling (`external`, `external_gte`, `internal`, `force`), case-insensitive. Any other value fails at startup. Only `External` and `ExternalGte` send a version with each operation, which enables optimistic concurrency control: OpenSearch rejects a write if the incoming version is not greater than (or, for `ExternalGte`, greater than or equal to) the stored version, and that document fails.

The version for each document is chosen as follows:

1. If `indexer.versionField` is set and the document has that field, its value (first value, if multi-valued) is read as a Long. A value that is not numeric is read as `0`.
2. Otherwise, if the document is a `KafkaDocument` (distributed mode), its Kafka offset is used.
3. Otherwise, no version is sent and the document is indexed without a version check.

```hocon
indexer {
  type: "OpenSearch"
  versionType: "ExternalGte"
  versionField: "my_version_field"
}
```

Setting `indexer.versionField` without `indexer.versionType` fails at startup.

---

## Partial Update Mode

When `update: true`, each document is sent as a bulk `update` operation instead of an `index` operation:

- **Full index** (default): Replaces the entire document in the index. Fields not present in the new document are removed.
- **Partial update**: Merges fields into the existing document. Fields not present in the update are left unchanged. No upsert is set, so an update to an ID that does not exist in the index fails for that document.

```hocon
opensearch {
  url: ${OPENSEARCH_URL}
  index: "my-index"
  update: true
}
```

---

## Index Override

OpenSearch supports `indexer.indexOverrideField` for routing documents to different indices within the same batch. The value of the override field on each document determines the target index; documents without the field go to `index`. The override field is not removed from the document. Add it to `indexer.blacklist` if it should not be indexed. Deletes (by ID and by query) are sent to the same per-document index. Each distinct value gets its own batch (see [Batching Behavior]({{< relref "docs/ingest-design/indexers#batching-behavior" >}})). If two documents in one send have the same ID but different target indices and the bulk response rejects one of them, the failure is attributed to whichever of the two was added last.

---

## Retry Behavior

Retries follow the generic `indexer.maxRetries` settings (see [Retries]({{< relref "docs/ingest-design/indexers#retries" >}})). For this indexer:

- A request that fails with an HTTP error carries that status code; a transport failure (connection refused, socket timeout) carries `-1`. The batch is retried if the code is in `indexer.retryableStatusCodes`, and otherwise fails as a whole.
- A document rejected in the bulk response carries the item's HTTP status code. If any rejected document's status is retryable (for example 429 but not 400), the whole batch is resent; once retries run out, or if none is retryable, only the rejected documents fail.
- A bulk response error whose ID does not match a document in the batch fails the whole batch and is not retried.
- An error on any delete-by-ID item, or any failure reported by delete-by-query, fails the whole batch and is not retried. Documents uploaded earlier in the same send were already indexed but still get FAIL events.

---

## Child Documents

When `childDocumentsField` is set, attached children are written into that field of the parent as a list of objects. They are not indexed as separate OpenSearch documents. The whitelist and blacklist apply to each child, and children of children are dropped. Map the field as `nested` if you need to query children individually.

If a document has children and `childDocumentsField` is not set, the children are not indexed and a warning is logged.

---

## Deletion

Supports delete by ID and delete by query using the generic `deletionMarkerField`, `deletionMarkerFieldValue`, `deleteByFieldField`, and `deleteByFieldValue` settings (see [Deletion Mechanics]({{< relref "docs/ingest-design/indexers#deletion-mechanics" >}})). Delete-by-query documents in a batch are combined into one `terms` bool/should query per target index, so a document matching any of them is deleted. The value to match is the first value of the `deleteByFieldValue` field, if it is multi-valued.

Within a batch, only the last document for a given index and ID is acted on: if the same ID appears in both an upload and a delete by ID, the last occurrence wins, and earlier documents with that ID are not sent but are still reported as succeeded. A document marked for delete-by-query also cancels an earlier upload with the same ID in the batch, but its query is not cancelled by a later upload with that ID, because delete-by-query requests are grouped by field rather than by ID. Each send then runs, in order, one bulk request for uploads, one bulk request for deletes by ID, and one delete-by-query per index. Because delete-by-query runs last, it deletes matching documents uploaded earlier in the same batch, whatever their order in the batch.

---

## Troubleshooting

**"index not found":**
The startup check only pings the cluster; Lucille does not check that the index exists and never creates it. Create the index (or an index template) before indexing so it has the mapping you need.

**TLS certificate errors:**
Set `acceptInvalidCert: true` for development environments with self-signed certificates. Do not use this in production.

**Authentication:**
Include credentials in the URL: `https://user:password@host:9200`. Use environment variable substitution to avoid hardcoding: `url: ${OPENSEARCH_URL}`.
