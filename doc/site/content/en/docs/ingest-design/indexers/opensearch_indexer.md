---
title: OpenSearch Indexer
weight: 2
date: 2025-06-09
description: Configuration reference for the OpenSearch Indexer.
---

`com.kmwllc.lucille.opensearch.indexer.OpenSearchIndexer`

Config block: `indexer.opensearch { ... }`.

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-opensearch</artifactId>
  <version>${lucille.version}</version>
</dependency>
```

The block selects the indexer once `lucille-opensearch` is on the classpath.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `url` | String or List\<String\> | Yes | OpenSearch endpoint URL, or a list of endpoints for the same cluster; with a list, requests are spread across the hosts. Credentials in the URL (e.g., `https://admin:password@localhost:9200`) are used for basic auth for that host. Only the scheme, host, and port are used; a path in the URL is ignored. |
| `index` | String | Yes | Target index name. |
| `update` | Boolean | No | Send partial update operations instead of full index operations; a document missing from the index is created. Default: `false`. |
| `acceptInvalidCert` | Boolean | No | Accept invalid TLS certificates and skip hostname verification. Default: `false`. |
| `useCompression` | Boolean | No | Enable request compression in the HTTP client. Default: `false`. |
| `childDocumentsField` | String | No | Field in the indexed document that holds attached child documents. If not set, attached children are not indexed. |
| `versionType` | String | No | Versioning type for writes. Takes a client `VersionType` name, case-insensitive, as the constant name (`External`, `ExternalGte`, `Internal`, `Force`) or its JSON spelling (`external`, `external_gte`, `internal`, `force`); any other value fails at startup. Only `External` and `ExternalGte` are sent with writes; `Internal` and `Force` are accepted but no version or version type is sent. |
| `versionField` | String | No | Document field holding a numeric version. Used when the document has it, else the Kafka offset of a document read from Kafka, else no version. A delete by ID uses only this field, never the Kafka offset. Fails at startup if it is set without `versionType`. |
| `routingField` | String | No | Document field whose value is sent as the routing key. |
| `splitOnRequestTooLarge` | Boolean | No | Split a bulk upload that OpenSearch rejects with HTTP 413 in half, and send each half on its own. See [Request Too Large](#request-too-large). Default: `false`. |
| `connectTimeoutMs` | Long | No | Maximum time to establish a connection, including the TLS handshake, in ms. Must be at least 1. Default: `5000`. |
| `socketTimeoutMs` | Long | No | Maximum time to wait for data on an open connection, including a response, in ms. Applies to delete-by-query too, so raise it above the slowest expected request, or set `-1` for no limit. Any other value below 1 fails at startup. A timeout is a transport failure (status `-1`), retried when `indexer.maxRetries` is set; OpenSearch may still complete the request. Default: `30000`. |
| `connectionTimeToLiveMs` | Long | No | Maximum lifetime of a pooled connection, in ms, so long-running clients behind a load balancer or Kubernetes Service reconnect and reach new nodes. Lower it to rebalance faster. Expiry is checked when a connection is next used, so it never interrupts a request. Must be at least 1. Default: `300000` (5 minutes). |
| `maxConnectionsPerRoute` | Integer | No | Maximum pooled connections to each host. Must be at least 1. Default: the larger of `5` and `indexer.maxConcurrentBatches` + 1. |
| `maxConnectionsTotal` | Integer | No | Maximum pooled connections across all hosts. Must be at least 1. Default: the larger of `25` and `maxConnectionsPerRoute`. |

Also reads `indexer.indexOverrideField` from the generic indexer block.

```hocon
indexer {
  opensearch {
    url: "https://admin:admin@localhost:9200"
    url: ${?OPENSEARCH_URL}
    index: "my-index"
    acceptInvalidCert: true
  }
}
```

An invalid connection setting (for example a timeout or connection limit below 1) fails at startup when the client is built. The startup connection check only pings the cluster; it does not check that `index` exists. With `indexer.sendEnabled: false`, no client is built and these checks are skipped.

The indexer supports `indexer.maxConcurrentBatches` greater than 1. All sends share one client, and the connection pool defaults above grow with the concurrency so sends do not wait for a connection. If you set `maxConnectionsPerRoute` yourself, keep it above `indexer.maxConcurrentBatches`.

---

## Routing

When `routingField` is set, the value of that field on each document (first value, if multi-valued) is used as the `_routing` parameter in the bulk request. A document without the field is sent without routing. Routing is not sent with deletes by ID or delete-by-query. This controls which shard receives the document. Use this when your OpenSearch index has custom shard routing configured.

---

## Version Type

`versionType` is an OpenSearch `VersionType`, as the constant name (`External`, `ExternalGte`, `Internal`, `Force`) or the JSON spelling (`external`, `external_gte`, `internal`, `force`), case-insensitive. Any other value fails at startup. Only `External` and `ExternalGte` send a version with each operation, which enables optimistic concurrency control: OpenSearch rejects a write with a 409 if the incoming version is not greater than (or, for `ExternalGte`, greater than or equal to) the stored version. That document is reported as succeeded, because the index already holds that version or a newer one: either a newer write superseded it, or a retry resent an upload that was applied but whose response was lost. A 409 on an upload sent without a version (no `versionType` of `External` or `ExternalGte`, or no version for that document) is a concurrent-modification conflict, and that document fails.

The version for each document is chosen as follows:

1. If `versionField` is set and the document has that field, its value (first value, if multi-valued) is read as a Long. A value that is not numeric is read as `0`.
2. Otherwise, if the document is a `KafkaDocument` (distributed mode), its Kafka offset is used.
3. Otherwise, no version is sent and the document is indexed without a version check.

Each rejected write is logged at INFO level ("already applied or superseded"), since it is reported as succeeded.

{{% alert title="Kafka offsets as versions" color="warning" %}}
Kafka offsets are only comparable within one partition of one topic. Lucille keys the messages the indexer reads by document ID, so every version of a document normally goes to the same partition, in order, and its offsets increase. Offsets stop being comparable when the topic's partition count changes, when the topic is deleted and recreated or the pipeline is renamed (offsets start again near 0), or when `idOverride` writes documents with different IDs under the same destination ID. In those cases a newer write can carry a lower version than the one in the index. OpenSearch rejects it with a 409, which is reported as succeeded, so the update is silently lost. If any of these can happen, set `versionField` to a version that increases across them, such as a source modification time.
{{% /alert %}}

```hocon
indexer {
  opensearch {
    url: "http://localhost:9200"
    index: "my-index"
    versionType: "ExternalGte"
    versionField: "my_version_field"
  }
}
```

Setting `versionField` without `versionType` fails at startup.

Deletes by ID with `External` or `ExternalGte` use only step 1: a delete document with `versionField` is sent with that version, and one without it is sent without a version. The Kafka offset is never used for a delete, because it is not comparable to a `versionField` value. OpenSearch checks a delete's version the same way as a write's. A delete it rejects (409) is reported as succeeded, because the index already holds that version or a newer one: either a newer document superseded the delete, or a retry resent a delete that was applied but whose response was lost. For as long as the index keeps deleted versions (`index.gc_deletes`, 60 seconds by default), OpenSearch also rejects an upload with an older version than the delete. Delete-by-query is never versioned.

---

## Partial Update Mode

When `update: true`, each document is sent as a bulk `update` operation instead of an `index` operation:

- **Full index** (default): Replaces the entire document in the index. Fields not present in the new document are removed.
- **Partial update**: Merges fields into the existing document. Fields not present in the update are left unchanged. The update is sent with `doc_as_upsert`, so a document whose ID is not in the index is created from its fields.

```hocon
indexer {
  opensearch {
    url: ${OPENSEARCH_URL}
    index: "my-index"
    update: true
  }
}
```

---

## Index Override

OpenSearch supports `indexer.indexOverrideField` for routing documents to different indices within the same batch. The value of the override field on each document determines the target index; documents without the field go to `index`. The override field is not removed from the document. Add it to `indexer.excludeFields` if it should not be indexed. Deletes (by ID and by query) are sent to the same per-document index. Each distinct value gets its own batch (see [Batching Behavior]({{< relref "docs/ingest-design/indexers#batching-behavior" >}})). If two documents in one send have the same ID but different target indices and the bulk response rejects one of them, the failure is attributed to whichever of the two was added last.

---

## Retry Behavior

Retries follow the generic `indexer.maxRetries` settings (see [Retries]({{< relref "docs/ingest-design/indexers#retries" >}})). For this indexer:

- A request that fails with an HTTP error carries that status code; a transport failure (connection refused, socket timeout) carries `-1`. A document rejected in a bulk response (an upload or a delete by ID) carries the item's HTTP status code, so a retry resends only the documents rejected with a retryable status (for example 429 but not 400).
- Each document is reported on its own. If a request fails as a whole after an earlier request in the same send was applied, only the documents in that request fail: documents already indexed are reported as succeeded and are not sent again. If nothing in the send has been applied yet, the whole send fails, and is retried if its status code is retryable. Failures reported by a delete-by-query fail the documents in that request and are not retried.
- Delete-by-query is sent as one request per target index, and each index's documents are reported on their own, so a retry doesn't resend a delete-by-query that another index already applied, which could delete documents written since.
- A batch is sent in order (see [Deletion](#deletion)). After a part of it has a retryable failure, the rest is not sent on that attempt and stays pending, so the retry resumes in batch order and a delete-by-query is never sent ahead of an earlier upload that is being retried. The final attempt sends everything left, since no retry follows it.
- A bulk response error that doesn't line up with the operation sent in its position can't be matched to a document, so it fails the whole request, which is not retried. Every document in that request gets a FAIL event, including any that OpenSearch applied. A 409 on a versioned upload or delete is not an error (see [Version Type](#version-type)), but only when its item lines up.
- A request whose response is lost (for example, a socket timeout) after OpenSearch applied it is sent again by the retry, since the indexer can't know it was applied. With `External`, a resent versioned upload or delete gets a 409 and counts as succeeded. With `ExternalGte`, a resent upload is an equal-version write and is applied again. That only matters if something outside this indexer deleted the document with a newer version in between and that delete's tombstone has expired (`index.gc_deletes`): the resent upload then re-creates the document. A resent delete by ID or delete-by-query runs again, and can remove a document written in between by another writer. Lucille doesn't send a batch while an in-flight batch writes any of the same IDs, so its own later writes aren't affected.

### Request Too Large

OpenSearch rejects a request body over `http.max_content_length` (100MB by default) with HTTP 413. `indexer.batchByteSize` keeps bulk requests below a size, but it is an estimate. By default, a 413 fails the whole upload request: it arrives without a parseable error body, so it is reported as a transport failure (status `-1`), and is retried when `-1` is in `indexer.retryableStatusCodes`.

With `splitOnRequestTooLarge: true`, a 413 on a bulk upload is recognized from the response, and the upload is split in half and each half sent on its own, recursively, until each part is accepted. A single document that still gets a 413 fails permanently by itself and is not retried. Once an upload is split, each half's failures stay with that half's documents. Deletes are never split.

---

## Child Documents

When `childDocumentsField` is set, attached children are written into that field of the parent as a list of objects. They are not indexed as separate OpenSearch documents. `includeFields` and `excludeFields` apply to each child, and children of children are dropped. Map the field as `nested` if you need to query children individually.

If a document has children and `childDocumentsField` is not set, the children are not indexed and a warning is logged.

---

## Deletion

Supports delete by ID and delete by query using the generic `deletionMarker` and `deleteByQuery` settings (see [Deletion Mechanics]({{< relref "docs/ingest-design/indexers#deletion-mechanics" >}})). Delete-by-query documents in a batch are combined into one `terms` bool/should query per target index, so a document matching any of them is deleted. The value to match is the first value of the field `deleteByQuery.valueField` names, if it is multi-valued. A delete-by-query document whose field name or value is null can't be part of a `terms` query, so it fails on its own without failing the rest of its batch; an earlier upload of the same ID in the batch is still sent.

Delete-by-query documents are sent in batch order: the uploads and deletes by ID before a delete-by-query document are sent before its query, and the ones after it are sent after. Consecutive delete-by-query documents are combined into one delete-by-query per index. Between two delete-by-query documents, only the last document for a given index and ID is acted on: if the same ID appears in both an upload and a delete by ID, the last occurrence wins, and earlier documents with that ID are not sent but are still reported as succeeded. A delete-by-query document doesn't affect an earlier upload with the same ID: the upload is sent before the query, and the query deletes it only if it matches. Each run of uploads and deletes by ID is sent as one bulk request for uploads and one for deletes by ID. Before each delete-by-query, the indexer refreshes the target index, because delete-by-query only matches refreshed documents. So a delete-by-query deletes matching documents uploaded before it, in the same batch or an earlier one, but not documents after it in the batch, such as new children sent after a delete of their parent's old children. Each delete-by-query adds a refresh and more bulk requests to the send, so frequent delete-by-query documents on a large index are slow.

---

## Bulk Request Body

Each document's body is written straight from the document's JSON tree into the client's JSON generator while the bulk request is serialized, rather than copied into a map first. The JSON sent is the same as a map of the document would give: fields excluded by `indexer.includeFields`/`indexer.excludeFields` are left out, the `id` field carries the document's ID (or its `indexer.idOverrideField` value), children go under `childDocumentsField`, and fields with a null value are left out at every depth, while nulls inside arrays are kept. This saves building a copy of every document in the batch, which matters for large batches. It requires the client's default Jackson JSON mapper.

---

## Troubleshooting

**"index not found":**
The startup check only pings the cluster; Lucille does not check that the index exists and never creates it. Create the index (or an index template) before indexing so it has the mapping you need.

**TLS certificate errors:**
Set `acceptInvalidCert: true` for development environments with self-signed certificates. Do not use this in production.

**Authentication:**
Include credentials in the URL: `https://user:password@host:9200`. Use environment variable substitution to avoid hardcoding: `url: ${OPENSEARCH_URL}`.
