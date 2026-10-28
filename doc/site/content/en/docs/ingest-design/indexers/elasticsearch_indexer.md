---
title: Elasticsearch Indexer
weight: 3
date: 2025-06-09
description: Configuration reference for the Elasticsearch Indexer, including join field support.
---

`com.kmwllc.lucille.indexer.ElasticsearchIndexer`

Config block: `elasticsearch { ... }`. Select it with `indexer.type: "Elasticsearch"`.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `url` | String or List\<String\> | Yes | Elasticsearch endpoint URL, or a list of endpoints for the same cluster; with a list, requests are spread across the hosts. Credentials in the URL (`https://user:password@host:9200`) are used for basic auth for that host. Only the scheme, host, and port are used; a path in the URL is ignored. |
| `index` | String | Yes | Target index name. |
| `update` | Boolean | No | Send partial update operations instead of full index operations. Default: `false`. |
| `acceptInvalidCert` | Boolean | No | Accept invalid TLS certificates and skip hostname verification. Default: `false`. |
| `useCompression` | Boolean | No | Enable request compression in the HTTP client. Default: `false`. |
| `connectTimeoutMs` | Long | No | Maximum time to establish a connection, including the TLS handshake, in ms. Must be at least 1. Default: `5000`. |
| `socketTimeoutMs` | Long | No | Maximum time to wait for data on an open connection, including a response, in ms. Applies to delete-by-query too, so raise it above the slowest expected request, or set `-1` for no limit. Any other value below 1 fails at startup. A timeout is a transport failure (status `-1`), retried when `indexer.maxRetries` is set; Elasticsearch may still complete the request. Default: `30000`. |
| `connectionTimeToLiveMs` | Long | No | Maximum lifetime of a pooled connection, in ms, so long-running clients behind a load balancer or Kubernetes Service reconnect and reach new nodes. Lower it to rebalance faster. Expiry is checked when a connection is next used, so it never interrupts a request. Must be at least 1. Default: `300000` (5 minutes). |
| `maxConnectionsPerRoute` | Integer | No | Maximum pooled connections to each host. Must be at least 1. Default: the larger of `5` and `indexer.maxConcurrentBatches` + 1. |
| `maxConnectionsTotal` | Integer | No | Maximum pooled connections across all hosts. Must be at least 1. Default: the larger of `25` and `maxConnectionsPerRoute`. |
| `childDocumentsField` | String | No | Field in the indexed document that holds attached child documents. If not set, attached children are not indexed. |
| `join` | Object | No | Parent-child join settings. See [Join Field Support](#join-field-support). |

```hocon
elasticsearch {
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
  routingField: "parent_id_field"
}
```

An invalid connection setting (for example a timeout or connection limit below 1) fails at startup when the client is built. The startup connection check only pings the cluster; it does not check that `index` exists. With `indexer.sendEnabled: false`, no client is built and these checks are skipped.

The indexer supports `indexer.maxConcurrentBatches` greater than 1. All sends share one client, and the connection pool defaults above grow with the concurrency so sends do not wait for a connection. If you set `maxConnectionsPerRoute` yourself, keep it above `indexer.maxConcurrentBatches`.

---

## Join Field Support

Elasticsearch's join field allows parent-child relationships within a single index. The indexer sets the join field on each uploaded document from the `elasticsearch.join` block:

| Parameter | Type | Required | Description |
|---|---|---|---|
| `joinFieldName` | String | Yes | Name of the join field in your Elasticsearch mapping. Required when `join` is set. |
| `isChild` | Boolean | No | Whether documents indexed by this indexer are children in the join relation. Default: `false`. |
| `childName` | String | No | Child relation name (must match your mapping). Required when `isChild` is `true`. |
| `parentDocumentIdSource` | String | No | Document field that holds the parent document's ID. Required when `isChild` is `true`. |
| `parentName` | String | No | Parent relation name (must match your mapping). Required when `isChild` is `false`. |

A missing required join key fails at startup.

- When `isChild` is `false`, the join field is set to the `parentName` string.
- When `isChild` is `true`, the join field is set to `{ "name": <childName>, "parent": <value of parentDocumentIdSource> }`. If the document lacks that field, `parent` is null.

The join field is added to the Lucille document itself before field filtering, so it is subject to `indexer.whitelist` and `indexer.blacklist`: include `joinFieldName` in a whitelist or it is not sent. It is applied in partial update mode too.

If a document already has a field named `joinFieldName`, the indexer throws and the whole batch fails without being retried. Because the join field stays on the document after the first attempt, this also happens whenever a batch is retried, so with `join` set a retry fails the batch instead of resending it.

The join settings do not set routing. Elasticsearch requires a child to be on the same shard as its parent, so set `indexer.routingField` to the same field as `parentDocumentIdSource`.

---

## Routing and Versioning

These use the generic `indexer` block:

- `indexer.routingField`: the value of this document field (first value, if multi-valued) is sent as the `_routing` parameter. A document without the field is sent without routing. Routing is not sent with deletes by ID or delete-by-query.
- `indexer.versionType`: an Elasticsearch `VersionType`, as the constant name (`External`, `ExternalGte`, `Internal`, `Force`) or the JSON spelling (`external`, `external_gte`, `internal`, `force`), case-insensitive. Any other value fails at startup. Only `External` and `ExternalGte` send a version; `Internal` and `Force` send none.
- `indexer.versionField`: with `External` or `ExternalGte`, the version is this field's value (first value, if multi-valued) read as a Long when it is set and present on the document; a value that is not numeric is read as `0`. Otherwise the Kafka offset is used for a document read from Kafka (distributed mode), and a document with neither is indexed without a version. Setting `indexer.versionField` without `indexer.versionType` fails at startup.

---

## Partial Update Mode

When `update: true`, each document is sent as a bulk `update` operation with the document's fields as the partial doc, instead of an `index` operation. No upsert is set, so an update to an ID that does not exist in the index fails for that document.

---

## Child Documents

When `childDocumentsField` is set, attached children are written into that field of the parent as a list of objects. They are not indexed as separate Elasticsearch documents. The whitelist and blacklist apply to each child, and children of children are dropped. Map the field as `nested` if you need to query children individually.

If a document has children and `childDocumentsField` is not set, the children are not indexed and a warning is logged.

---

## Index Override

`indexer.indexOverrideField` is not supported. Setting it fails at startup. All documents go to the configured `index`.

---

## Retry Behavior

Retries follow the generic `indexer.maxRetries` settings (see [Retries]({{< relref "docs/ingest-design/indexers#retries" >}})). For this indexer:

- A request that fails with an HTTP error carries that status code; a transport failure (connection refused, socket timeout) carries `-1`. The batch is retried if the code is in `indexer.retryableStatusCodes`, and otherwise fails as a whole.
- A document rejected in the bulk response carries the item's HTTP status code. If any rejected document's status is retryable (for example 429 but not 400), the whole batch is resent; once retries run out, or if none is retryable, only the rejected documents fail.
- A bulk response error whose ID does not match a document in the batch fails the whole batch and is not retried.
- An error on any delete-by-ID item, or any failure reported by delete-by-query, fails the whole batch and is not retried. Documents uploaded earlier in the same send were already indexed but still get FAIL events.
- With `join` set, a retry fails the batch (see [Join Field Support](#join-field-support)).

---

## Deletion Support

The ElasticsearchIndexer supports both generic deletion mechanisms. (The indexer logs a warning at startup that deletion is not supported when deletion fields are set; deletion is performed regardless.)

**Delete by ID:** When a document is marked with `deletionMarkerField`/`deletionMarkerFieldValue` (and does not also have `deleteByFieldField`/`deleteByFieldValue`), the Indexer issues a bulk delete-by-ID operation for that document.

**Delete by query:** When a document is marked for deletion AND contains both `deleteByFieldField` and `deleteByFieldValue`, the Indexer issues a `_delete_by_query` request. The `deleteByFieldField` value on the document identifies which Elasticsearch field to query, and the `deleteByFieldValue` value identifies the term to match (first value, if multi-valued). Multiple delete-by-query documents in a batch are combined into a single `terms` bool/should query, so a document matching any of them is deleted.

**Batch ordering:** Within a batch, only the last document for a given ID is acted on: if the same ID appears in both an upload and a delete by ID, the last occurrence wins, and earlier documents with that ID are not sent but are still reported as succeeded. A document marked for delete-by-query also cancels an earlier upload with the same ID in the batch, but its query is not cancelled by a later upload with that ID, because delete-by-query requests are grouped by field rather than by ID. Each send then runs, in order, one bulk request for uploads, one bulk request for deletes by ID, and one delete-by-query. Because delete-by-query runs last, it deletes matching documents uploaded earlier in the same batch, whatever their order in the batch.

```hocon
indexer {
  type: "Elasticsearch"
  deletionMarkerField: "is_deleted"
  deletionMarkerFieldValue: "true"
  # Optional: for query-based deletions
  deleteByFieldField: "delete_target_field"
  deleteByFieldValue: "delete_target_value"
}
```

---

## Troubleshooting

**Join field errors:**
Ensure your Elasticsearch index mapping includes the join field with the correct parent and child relation names. The `joinFieldName` in config must match the mapping exactly.

**Routing errors with joins:**
Parent and child documents must be on the same shard. Set `indexer.routingField` to the field containing the parent ID.

**"index not found":**
The startup check only pings the cluster; Lucille does not check that the index exists and never creates it. Create the index (or an index template) before indexing so it has the mapping you need.
