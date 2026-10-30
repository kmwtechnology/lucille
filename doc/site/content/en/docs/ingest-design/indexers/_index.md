---
title: Indexers
weight: 5
date: 2025-06-09
description: Configuration reference for built-in and plugin indexers shipped with Lucille.
---

For conceptual documentation — what an Indexer is, why batching matters, deletion as a design pattern, and error handling at the batch level — see [Architecture: Indexer]({{< relref "docs/architecture/components/indexer" >}}).

## Generic `indexer` Configuration

Indexer configuration has two parts: the generic `indexer` block (common to all backends), and a backend-specific config block (e.g., `solr`, `opensearch`, `elasticsearch`, `csv`).

```hocon
indexer {
  type: "Solr"
  batchSize: 100
  batchTimeout: 100
  blacklist: ["internal_field"]
}

solr {
  useCloudClient: true
  url: "http://localhost:8983/solr"
  defaultCollection: "my-collection"
}
```

`indexer.type` is a shorthand that is resolved at runtime from the classpath: `"Solr"`, `"OpenSearch"`, `"Elasticsearch"`, `"CSV"`, or `"Nop"`. 
`Solr`, `OpenSearch`, and `Elasticsearch` live in their own plugin modules and the `type` shortcut only resolves when that module is on the classpath (see the per-indexer pages below for the exact Maven dependency). 
For plugin indexers that do not register a `type` (e.g., Pinecone, Weaviate), use `indexer.class` with the fully qualified class name instead. `indexer.class` also works for any of the backends above if you prefer to be explicit.

Both blocks are validated when the indexer is constructed: an unknown key in the `indexer` block or in the backend block fails at startup. After construction, Lucille calls the indexer's connection check (for example a ping) and the run fails at startup if it does not succeed.

### Generic Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `type` | String | No | Shorthand from the classpath (case-insensitive): `Solr`, `OpenSearch`, `Elasticsearch` (each requires its plugin module on the classpath), or `CSV` / `Nop` (in `lucille-core`). Defaults to Nop. |
| `class` | String | No | Fully qualified class name for plugin or custom indexers. Takes precedence over `type` if both are set. |
| `batchSize` | Integer | No | Number of documents a batch holds before it is sent. If only `batchByteSize` is set, count-based batching is disabled. Default: `100`. |
| `batchByteSize` | Long | No | Estimated size in bytes a batch may hold before it is sent. The size is estimated from the document's JSON structure, not measured from the serialized request. When set alone, count-based batching is disabled. When set alongside `batchSize`, whichever limit is reached first sends the batch. Default: disabled. |
| `batchTimeout` | Integer | No | Milliseconds since the last add or flush after which a partly filled batch is sent. Default: `100`. |
| `maxConcurrentBatches` | Integer | No | Maximum number of batches in flight to the destination at once. `1` sends each batch synchronously on the indexer thread. Values below 1 fail at startup. Values above 1 are accepted only by the Solr, OpenSearch, and Elasticsearch indexers (and NopIndexer); other indexers fail at startup. See [Concurrent Batches](#concurrent-batches). Default: `1`. |
| `idOverrideField` | String | No | Document field whose value is used as the ID at the destination instead of `id`. The document's own `id` is not changed. If a document lacks the field, its `id` is used. A multi-valued field uses its first value. |
| `indexOverrideField` | String | No | Document field whose value selects the target index or collection for that document. Enables per-index batching. Used by the Solr and OpenSearch indexers. The Elasticsearch, CSV, Pinecone, and Weaviate indexers fail at startup if it is set. |
| `whitelist` | List\<String\> | No | Only these fields are sent to the destination. Fields on the blacklist are still excluded. |
| `blacklist` | List\<String\> | No | These fields are never sent to the destination. |
| `sendEnabled` | Boolean | No | Set to `false` to run without sending anything to the destination. The Solr, OpenSearch, Elasticsearch, Pinecone, and Weaviate indexers then skip creating their client and their connection check, and report every document as succeeded. The CSV indexer ignores it and still writes the file. Default: `true`. |
| `deletionMarkerField` | String | No | Field that marks a document for deletion when its value equals `deletionMarkerFieldValue`. Setting only one of the two fails at startup. |
| `deletionMarkerFieldValue` | String | No | Value of `deletionMarkerField` that marks a document for deletion. Compared with the field's value as a string (first value, if multi-valued), so `"true"` matches a boolean `true` field. |
| `deleteByFieldField` | String | No | Name of a document field whose value is the destination field to match in a delete-by-query. Setting only one of `deleteByFieldField` and `deleteByFieldValue` fails at startup. |
| `deleteByFieldValue` | String | No | Name of a document field whose value is the value to match in a delete-by-query. |
| `maxRetries` | Integer | No | Maximum number of retries for a failed batch, after the first attempt. Must be greater than 0 when set (0 or less fails at startup). Omit it to disable retries. Default: retries disabled. |
| `retryWaitDurationMs` | Integer | No | Wait before the first retry, in ms. Each later wait is 1.5 times the previous one (before jitter). Setting it without `maxRetries` fails at startup. Default: `1000`. |
| `retryMaxWaitDurationMs` | Long | No | Upper bound on the wait between retries, in ms. Setting it without `maxRetries` fails at startup. Default: `30000`. |
| `retryRandomizationFactor` | Double | No | Jitter applied to each wait. `0.5` means the actual wait is between 50% and 150% of the computed backoff; `0.0` disables jitter. Setting it without `maxRetries` fails at startup. Default: `0.5`. |
| `retryableStatusCodes` | List\<Integer\> | No | HTTP status codes that trigger a retry. `-1` stands for a failure with no status code (for example a connection error or timeout). An empty list fails at startup, and so does setting it without `maxRetries`. Default: `[429, 503, -1]`. |
| `versionType` | String | No | Versioning type for writes. Read only by the OpenSearch and Elasticsearch indexers, which take a client `VersionType` name, case-insensitive, as the constant name (`External`, `ExternalGte`, `Internal`, `Force`) or its JSON spelling (`external`, `external_gte`, `internal`, `force`); any other value fails at startup there. Only `External` and `ExternalGte` are sent with writes; `Internal` and `Force` are accepted but no version or version type is sent. See each indexer's page for where the version number comes from. |
| `versionField` | String | No | Document field holding a numeric version. Read only by the OpenSearch and Elasticsearch indexers, which use it when the document has it, else the Kafka offset of a document read from Kafka, else no version. Both fail at startup if it is set without `versionType`. Other indexers ignore it. |
| `routingField` | String | No | Document field whose value is sent as the routing key. Read only by the OpenSearch and Elasticsearch indexers. |
| `pipeline` | String | No | Pipeline to index for when the indexer is started on its own (`com.kmwllc.lucille.core.Indexer` main, reading from Kafka) without a pipeline argument. A pipeline argument takes precedence. Not read otherwise. |

### Field Filtering

Whitelist and blacklist are applied **at indexing time**, not during pipeline processing. Stages see all fields on a document; only the Indexer strips fields before sending to the backend. When both are set, a field is sent only if it is on the whitelist and not on the blacklist. Dropped documents never reach the Indexer.

The filter applies to every top-level field, including `id` and `run_id`. `run_id` is sent as a regular field unless you blacklist it (or leave it off the whitelist). For the OpenSearch and Elasticsearch indexers, filtering `id` out removes it from the document body only; the destination ID is still set from `id` or `idOverrideField`. The Solr indexer sends the document with no `id` field, even when `idOverrideField` is set. The filter is also applied to each child document.

The Solr, OpenSearch, Elasticsearch, Pinecone, and Weaviate indexers apply `whitelist`, `blacklist`, and `idOverrideField`. The Pinecone indexer filters before selecting `metadataFields` and writes the `idOverrideField` value as the vector ID; the Weaviate indexer writes it to `idDestinationName` and derives the object UUID from it. The CSV indexer ignores all three.

`___children` is never sent as a regular field. The Solr indexer sends children as nested child documents; the OpenSearch and Elasticsearch indexers embed them in `childDocumentsField` when it is set, and otherwise skip them with a warning. Children of children are dropped by all three. The Pinecone and Weaviate indexers do not send children.

### Deletion Mechanics

Two distinct deletion mechanisms are available:

**Delete by ID:** Set `deletionMarkerField` and `deletionMarkerFieldValue`. When a document's marker field (first value, if multi-valued) equals the marker value, the Indexer deletes that document's ID (or its `idOverrideField` value) at the destination instead of indexing it.

**Delete by query:** Also set `deleteByFieldField` and `deleteByFieldValue`. When a document marked for deletion also has both fields named by `deleteByFieldField` and `deleteByFieldValue`, the Indexer issues a delete-by-query instead of a delete by ID: it deletes all documents in the index whose field named by the document's `deleteByFieldField` value matches the document's `deleteByFieldValue` value. A marked document that lacks either field is deleted by ID.

```hocon
indexer {
  type: "Solr"
  deletionMarkerField: "file_expired"
  deletionMarkerFieldValue: "true"
  deleteByFieldField: "delete_by_field"
  deleteByFieldValue: "delete_by_value"
}
```

Deletion is performed by the Solr, OpenSearch, and Elasticsearch indexers. The Pinecone indexer supports delete by ID only. The CSV indexer does not support deletion: it logs a warning and writes documents marked for deletion as regular rows. The Weaviate indexer likewise logs a warning and indexes them as regular documents.

### Batching Behavior

Documents accumulate in a batch, which is sent when:
- It holds `batchSize` documents and another document arrives. The full batch is sent and the new document starts the next one.
- Adding the next document would take the estimated size over `batchByteSize`. The batch is sent without that document, so a batch stays within the limit unless a single document exceeds it, in which case that document is sent alone.
- `batchTimeout` milliseconds have passed since the last add or flush. Expiry is checked when a document is added and whenever the indexer polls and finds nothing.
- The indexer stops: the open batch is sent and in-flight batches are completed.

Time the indexer spends blocked sending a batch or waiting for a free concurrency slot does not count toward `batchTimeout`, so a slow send does not cause the next batch to be sent partly filled.

When only `batchByteSize` is set (without `batchSize`), count-based batching is disabled and batches are sent purely on size. When both are set, whichever limit is reached first sends the batch. This is useful for backends with request-size limits (e.g., a 10 MB bulk API limit) where a fixed document count may produce unpredictably sized payloads. The size estimate covers the whole document, including fields the whitelist or blacklist will remove and attached children.

When `indexOverrideField` is set, the Indexer keeps a separate batch per distinct field value (documents without the field share one batch for the default index). Each sub-batch follows the rules above, and whenever a document is added, any other sub-batch that has passed `batchTimeout` is sent along with it.

**Batch-level vs. per-document failures:** If the send throws (for example the whole bulk request is rejected or a delete fails), every document in the batch gets a FAIL event. Individual document rejections in a bulk response (e.g., mapping errors) fail only those documents; the rest get FINISH events.

### Retries

Retries are disabled unless `maxRetries` is set. When enabled, a batch is resent (the whole batch, including documents that already succeeded) when:
- the send throws a retryable exception whose status code is in `retryableStatusCodes`, or
- any document in the response failed with a status code in `retryableStatusCodes`.

Which failures carry a status code depends on the indexer; see each indexer's page. Once retries are exhausted, the last outcome is used: per-document failures get FAIL events for those documents only, and a thrown exception fails the whole batch. Each attempt counts toward [Kafka's poll interval](#kafka-offset-commits).

### Concurrent Batches

With `maxConcurrentBatches` greater than 1, sends run on a pool of that many threads while the indexer keeps polling and filling the next batch. The indexer client is shared by all sends; the OpenSearch and Elasticsearch connection pools are sized for it automatically (see `maxConnectionsPerRoute` on those pages).

Ordering is preserved where it matters:
- A batch is not sent while an in-flight batch writes any of the same destination IDs (including child IDs), so writes and deletes of one document apply in order.
- A batch containing a delete-by-query is sent alone: it waits for every in-flight batch to finish, and no other batch is sent until it finishes.
- Batches complete (FINISH and FAIL events, Kafka offset commits) in the order they were sent.

The setting is also rejected at startup for a custom messenger that commits its input when a document is polled rather than when its batch completes. All built-in messengers (local, Kafka, and hybrid) allow it.

### Kafka Offset Commits

In distributed mode the indexer reads from Kafka and commits a document's offset only after its batch completes, that is, after its FINISH or FAIL event is sent. (In hybrid mode the indexer likewise hands offsets back for commit only when a batch completes.) A document that failed to index is committed too and is not re-delivered. Offsets are committed up to the oldest document not yet complete in each partition, so a crash re-delivers anything uncommitted (at-least-once delivery).

In distributed mode, when a partition is about to be revoked in a consumer-group rebalance, the indexer sends its open batch and waits for in-flight batches before the handoff, so a clean rebalance does not re-deliver them. On a normal stop it also sends the open batch, drains in-flight batches, and commits.

While the indexer waits on a send it does not poll Kafka. `kafka.maxPollIntervalSecs` must therefore exceed about twice the slowest send including retries when `maxConcurrentBatches` is 1, and about three times when it is greater than 1. See [Kafka integration]({{< relref "docs/architecture/internals/kafka-integration" >}}).

A record on the destination topic that cannot be deserialized stops the indexer, unless `kafka.onDeserializationError` is `skip`. In `skip` mode the record is logged and skipped, but `kafka.maxConsecutiveDeserializationErrors` (default 10; `-1` for unlimited) bad records in a row on one partition still stop the indexer. A skipped record produces no completion event, so in a batch run the run waits until `runner.connectorTimeout` expires.

---

## Indexer Catalogue

### Core Indexers

- [CSV Indexer]({{< relref "docs/ingest-design/indexers/csv_indexer" >}}) — Write pipeline output to a CSV file.
- [NopIndexer]({{< relref "docs/ingest-design/indexers/nop_indexer" >}}) — No-op indexer for testing (default).

### Plugin Indexers

- [Solr Indexer]({{< relref "docs/ingest-design/indexers/solr_indexer" >}}) — Single-node and SolrCloud.
- [OpenSearch Indexer]({{< relref "docs/ingest-design/indexers/opensearch_indexer" >}}) — OpenSearch with optional partial updates.
- [Elasticsearch Indexer]({{< relref "docs/ingest-design/indexers/elasticsearch_indexer" >}}) — Elasticsearch with join field support.
- [Pinecone Indexer]({{< relref "docs/ingest-design/indexers/pinecone_indexer" >}}) — Index vector embeddings into Pinecone.
- [Weaviate Indexer]({{< relref "docs/ingest-design/indexers/weaviate_indexer" >}}) — Index documents and vectors into Weaviate.
