---
title: Indexers
weight: 5
date: 2025-06-09
description: Configuration reference for built-in and plugin indexers shipped with Lucille.
---

For conceptual documentation — what an Indexer is, why batching matters, deletion as a design pattern, and error handling at the batch level — see [Architecture: Indexer]({{< relref "docs/architecture/components/indexer" >}}).

## Generic `indexer` Configuration

Indexer configuration lives in the `indexer` block. It holds generic settings common to all backends, and a nested backend-specific block (e.g., `solr`, `opensearch`, `elasticsearch`, `csv`). The name of the nested block selects the indexer.

```hocon
indexer {
  batchSize: 100
  batchTimeoutMs: 100
  excludeFields: ["internal_field"]

  solr {
    useCloudClient: true
    url: "http://localhost:8983/solr"
    defaultCollection: "my-collection"
  }
}
```

The nested block's name is resolved at runtime from the classpath: `solr`, `opensearch`, `elasticsearch`, `csv`, `pinecone`, or `weaviate`. Backends other than `csv` live in their own plugin modules and resolve only when that module is on the classpath (see the per-indexer pages below for the exact Maven dependency).

Lucille chooses the indexer as follows:

| Config | Result |
|---|---|
| `indexer.class` is set | That class is used. |
| `indexer.type` is set | That type is used. |
| Neither, one nested block | The block's name selects the indexer. |
| Neither, no nested block | Fails at startup, unless no connector has a pipeline, since a [setup-only run]({{< relref "docs/ingest-design/control-flow#setup-only-connectors-no-pipeline" >}}) starts no indexer. To run with no destination, set `type: "Nop"`. |
| Neither, several nested blocks | Fails at startup. Set `indexer.type` to choose one. |
| The block's name matches no registered indexer | Fails at startup. The error lists the available types. |
| `indexer.class` or `indexer.type` is set, with a block the indexer does not use | Ignored with a warning if the block's name matches a registered indexer. Otherwise fails at startup, and the error lists the available types. |

Use `indexer.type` to choose among several blocks with one value, such as `type: ${?INDEXER_TYPE}`, or to state that a run has no destination with `type: "Nop"`. Use `indexer.class` for a custom indexer that does not register a type.

A top-level backend block is not read by the indexer. If the selected indexer has one and no nested block, startup fails with a message telling you to move it. To share settings between an indexer and a stage or connector, define them under a top-level key of your own and merge them into each block:

```hocon
esCluster {
  url: "https://es.example.com:9200"
  acceptInvalidCert: false
}

indexer {
  elasticsearch: ${esCluster} {
    index: "products"
  }
}

pipelines: [{
  name: "main"
  stages: [{
    class: "com.kmwllc.lucille.elasticsearch.stage.ElasticsearchLookup"
    elasticsearch: ${esCluster} {
      index: "reference_data"
    }
    source: ["price"]
    dest: ["es_price"]
  }]
}]
```

The generic settings and the backend block are validated when the indexer is constructed: an unknown key in the `indexer` block or in the backend block fails at startup. After construction, Lucille calls the indexer's connection check (for example a ping) and the run fails at startup if it does not succeed.

### Generic Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `type` | String | No | Selects the indexer by its registered type (case-insensitive): `Solr`, `OpenSearch`, `Elasticsearch`, `Pinecone`, `Weaviate` (each requires its plugin module on the classpath), `CSV`, or `Nop`. Usually not needed, because the nested block's name selects the indexer. |
| `class` | String | No | Fully qualified class name, for a custom indexer. Takes precedence over `type` if both are set. |
| `batchSize` | Integer | No | Number of documents a batch holds before it is sent. If only `batchByteSize` is set, count-based batching is disabled. Default: `100`. |
| `batchByteSize` | Long | No | Estimated size in bytes a batch may hold before it is sent. The size is estimated from the document's JSON structure, not measured from the serialized request. When set alone, count-based batching is disabled. When set alongside `batchSize`, whichever limit is reached first sends the batch. Default: disabled. |
| `batchTimeoutMs` | Integer | No | Milliseconds since the last add or flush after which a partly filled batch is sent. Default: `100`. |
| `maxConcurrentBatches` | Integer | No | Maximum number of batches in flight to the destination at once. `1` sends each batch synchronously on the indexer thread. Values below 1 fail at startup. Values above 1 are accepted only by the Solr, OpenSearch, and Elasticsearch indexers (and NopIndexer); other indexers fail at startup. See [Concurrent Batches](#concurrent-batches). Default: `1`. |
| `idOverrideField` | String | No | Document field whose value is used as the ID at the destination instead of `id`. The document's own `id` is not changed. If a document lacks the field, its `id` is used. A multi-valued field uses its first value. |
| `indexOverrideField` | String | No | Document field whose value selects the target index or collection for that document. Enables per-index batching. Used by the Solr and OpenSearch indexers. The Elasticsearch, CSV, Pinecone, and Weaviate indexers fail at startup if it is set. |
| `includeFields` | List\<String\> | No | Only these fields are sent to the destination. Fields in `excludeFields` are still excluded. |
| `excludeFields` | List\<String\> | No | These fields are never sent to the destination. |
| `sendEnabled` | Boolean | No | Set to `false` to run without sending anything to the destination. The Solr, OpenSearch, Elasticsearch, Pinecone, and Weaviate indexers then skip creating their client and their connection check, and report every document as succeeded. The CSV indexer writes no file and also reports every document as succeeded. Default: `true`. |
| `sendFailedDocsToTopic` | Boolean | No | Copy each document that fails to index to the pipeline's fail topic. Only takes effect in Kafka and Hybrid modes. See [The Fail Topic]({{< relref "docs/architecture/internals/error-handling#the-fail-topic-dead-letter-queue" >}}). Default: `false`. |
| `deletionMarker` | Object | No | Marks documents for deletion. `field` is the document field to check, and `value` the value of that field that marks a document for deletion. The value is compared with the field's value as a string (first value, if multi-valued), so `"true"` matches a boolean `true` field. Both keys are required. |
| `deleteByQuery` | Object | No | Turns a marked document into a delete-by-query. `fieldNameField` is the document field holding the name of the destination field to match, and `valueField` the document field holding the value to match. Both keys are required, and so is `deletionMarker`. |
| `maxRetries` | Integer | No | Maximum number of retries for a failed batch, after the first attempt. Must be greater than 0 when set (0 or less fails at startup). Omit it to disable retries. Default: retries disabled. |
| `retryWaitDurationMs` | Integer | No | Wait before the first retry, in ms. Each later wait is 1.5 times the previous one (before jitter). Setting it without `maxRetries` fails at startup. Default: `1000`. |
| `retryMaxWaitDurationMs` | Long | No | Upper bound on the wait between retries, in ms. Setting it without `maxRetries` fails at startup. Default: `30000`. |
| `retryRandomizationFactor` | Double | No | Jitter applied to each wait. `0.5` means the actual wait is between 50% and 150% of the computed backoff; `0.0` disables jitter. Setting it without `maxRetries` fails at startup. Default: `0.5`. |
| `retryableStatusCodes` | List\<Integer\> | No | HTTP status codes that trigger a retry. `-1` stands for a failure with no status code (for example a connection error or timeout). An empty list fails at startup, and so does setting it without `maxRetries`. Default: `[429, 503, -1]`. |
| `pipeline` | String | No | Pipeline to index for when the indexer is started on its own (`com.kmwllc.lucille.core.Indexer` main, reading from Kafka) without a pipeline argument. A pipeline argument takes precedence. Not read otherwise. |

### Field Filtering

`includeFields` and `excludeFields` are applied **at indexing time**, not during pipeline processing. Stages see all fields on a document; only the Indexer strips fields before sending to the backend. When both are set, a field is sent only if it is in `includeFields` and not in `excludeFields`. Dropped documents never reach the Indexer.

The filter applies to every top-level field, including `id` and `run_id`. `run_id` is sent as a regular field unless you add it to `excludeFields` (or leave it out of `includeFields`). For the OpenSearch and Elasticsearch indexers, filtering `id` out removes it from the document body only; the destination ID is still set from `id` or `idOverrideField`. The Solr indexer always sends the `id` field, set from `idOverrideField` or `id`, because Solr needs it on every document. The Weaviate indexer always writes the ID to its `idDestinationName` property. The filter is also applied to each child document.

The Solr, OpenSearch, Elasticsearch, Pinecone, and Weaviate indexers apply `includeFields`, `excludeFields`, and `idOverrideField`. The Pinecone indexer filters before selecting `metadataFields` and writes the `idOverrideField` value as the vector ID; the Weaviate indexer writes it to `idDestinationName` and derives the object UUID from it. The CSV indexer fails at startup if any of the three is set, because `columns` decides what it writes.

`___children` is never sent as a regular field. The Solr indexer sends children as nested child documents; the OpenSearch and Elasticsearch indexers embed them in `childDocumentsField` when it is set, and otherwise skip them with a warning. Children of children are dropped by all three. The Pinecone and Weaviate indexers do not send children.

### Deletion Mechanics

Two distinct deletion mechanisms are available:

**Delete by ID:** Set `deletionMarker`. When a document's `deletionMarker.field` (first value, if multi-valued) equals `deletionMarker.value`, the Indexer deletes that document's ID (or its `idOverrideField` value) at the destination instead of indexing it.

**Delete by query:** Also set `deleteByQuery`. When a document marked for deletion also has both fields that `deleteByQuery` names, the Indexer issues a delete-by-query instead of a delete by ID. For example, a marked document with `delete_by_field: "parent_id"` and `delete_by_value: "123"` deletes every document in the index whose `parent_id` is `123`. A marked document that lacks either field is deleted by ID.

```hocon
indexer {
  type: "Solr"
  deletionMarker {
    field: "file_expired"
    value: "true"
  }
  deleteByQuery {
    fieldNameField: "delete_by_field"
    valueField: "delete_by_value"
  }
}
```

Deletion is performed by the Solr, OpenSearch, and Elasticsearch indexers. The Pinecone indexer supports delete by ID only, and fails at startup if `deleteByQuery` is set. The CSV and Weaviate indexers do not support deletion, and fail at startup if `deletionMarker` or `deleteByQuery` is set.

### Batching Behavior

Documents accumulate in a batch, which is sent when:
- It holds `batchSize` documents and another document arrives. The full batch is sent and the new document starts the next one.
- Adding the next document would take the estimated size over `batchByteSize`. The batch is sent without that document, so a batch stays within the limit unless a single document exceeds it, in which case that document is sent alone.
- `batchTimeoutMs` milliseconds have passed since the last add or flush. Expiry is checked when a document is added and whenever the indexer polls and finds nothing.
- The indexer stops: the open batch is sent and in-flight batches are completed.

Time the indexer spends blocked sending a batch or waiting for a free concurrency slot does not count toward `batchTimeoutMs`, so a slow send does not cause the next batch to be sent partly filled.

When only `batchByteSize` is set (without `batchSize`), count-based batching is disabled and batches are sent purely on size. When both are set, whichever limit is reached first sends the batch. This is useful for backends with request-size limits (e.g., a 10 MB bulk API limit) where a fixed document count may produce unpredictably sized payloads. The size estimate covers the whole document, including fields that `includeFields` or `excludeFields` will remove and attached children.

When `indexOverrideField` is set, the Indexer keeps a separate batch per distinct field value (documents without the field share one batch for the default index). Each sub-batch follows the rules above, and whenever a document is added, any other sub-batch that has passed `batchTimeoutMs` is sent along with it.

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

In distributed mode the indexer reads from Kafka and commits a document's offset only after its batch completes, that is, after its FINISH or FAIL event is sent. (In hybrid mode the worker commits a source record's offset once the batches holding everything sent for indexing from it have completed.) A document that failed to index is committed too and is not re-delivered. Offsets are committed up to the oldest document not yet complete in each partition, so a crash re-delivers anything uncommitted (at-least-once delivery).

In distributed mode, when a partition is about to be revoked in a consumer-group rebalance, the indexer sends its open batch and waits for in-flight batches before the handoff, so a clean rebalance does not re-deliver them. On a normal stop it also sends the open batch, drains in-flight batches, and commits.

While the indexer waits on a send it does not poll Kafka. `kafka.maxPollIntervalMs` must therefore exceed about twice the slowest send including retries when `maxConcurrentBatches` is 1, and about three times when it is greater than 1. See [Kafka integration]({{< relref "docs/architecture/internals/kafka-integration" >}}).

A record on the destination topic that cannot be deserialized stops the indexer, unless `kafka.onDeserializationError` is `skip`. In `skip` mode the record is logged and skipped, but `kafka.maxConsecutiveDeserializationErrors` (default 10; `-1` for unlimited) bad records in a row on one partition still stop the indexer. A skipped record produces no completion event, so in a batch run the run waits until `runner.connectorTimeoutMs` expires.

---

## Indexer Catalogue

### Core Indexers

- [CSV Indexer]({{< relref "docs/ingest-design/indexers/csv_indexer" >}}) — Write pipeline output to a CSV file.
- [NopIndexer]({{< relref "docs/ingest-design/indexers/nop_indexer" >}}) — No-op indexer for testing.

### Plugin Indexers

- [Solr Indexer]({{< relref "docs/ingest-design/indexers/solr_indexer" >}}) — Single-node and SolrCloud.
- [OpenSearch Indexer]({{< relref "docs/ingest-design/indexers/opensearch_indexer" >}}) — OpenSearch with optional partial updates.
- [Elasticsearch Indexer]({{< relref "docs/ingest-design/indexers/elasticsearch_indexer" >}}) — Elasticsearch with join field support.
- [Pinecone Indexer]({{< relref "docs/ingest-design/indexers/pinecone_indexer" >}}) — Index vector embeddings into Pinecone.
- [Weaviate Indexer]({{< relref "docs/ingest-design/indexers/weaviate_indexer" >}}) — Index documents and vectors into Weaviate.
