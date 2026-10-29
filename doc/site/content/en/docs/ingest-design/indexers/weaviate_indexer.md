---
title: Weaviate Indexer
weight: 7
date: 2025-06-09
description: Configuration reference for the Weaviate Indexer, which indexes documents and vectors into Weaviate.
---

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

| Parameter | Type | Required | Description |
|---|---|---|---|
| `apiKey` | String | Yes | Weaviate API key for authentication. Use `${WEAVIATE_API_KEY}` for environment variable substitution. |
| `host` | String | Yes | Weaviate instance hostname (e.g., `my-cluster.weaviate.network`). Do not include the protocol. HTTPS is always used. |
| `className` | String | No | The Weaviate class (object type) that objects are written to. Default: `Document`. |
| `idDestinationName` | String | No | Property under which the document's ID (its Lucille ID, or the `indexer.idOverrideField` value when present) is stored in Weaviate, since Weaviate's `id` must be a UUID. Default: `id_original`. |
| `vectorField` | String | No | Document field containing the vector embedding to index, as a list of floats. If omitted, no vector is sent (useful if Weaviate is configured to generate its own embeddings). |

```hocon
indexer {
  class: "com.kmwllc.lucille.weaviate.indexer.WeaviateIndexer"
}

weaviate {
  apiKey: ${WEAVIATE_API_KEY}
  host: "my-cluster.weaviate.network"
  className: "Article"
  idDestinationName: "lucille_id"
  vectorField: "content_vector"
}
```

**Note on IDs:** Weaviate requires UUIDs for its internal `id` field. The WeaviateIndexer generates a UUID from the document's Lucille ID (or from `indexer.idOverrideField` when it is set and present) and stores that ID under `idDestinationName` so it can be retrieved later.

The generic `indexer.whitelist` and `indexer.blacklist` options control which fields are sent as object properties. `indexer.indexOverrideField` is not supported and fails at startup.

---

## UUID Generation

Weaviate requires UUIDs for its internal `id` field. The WeaviateIndexer generates a deterministic UUID from the document's ID (the `indexer.idOverrideField` value when it is set and present, otherwise the Lucille ID) using `UUID.nameUUIDFromBytes(id.getBytes())`. This means:

- The same ID always maps to the same Weaviate UUID, enabling idempotent upserts.
- That ID is stored under `idDestinationName` (default: `id_original`) so it can be retrieved later.

---

## Properties Sent

Every document field except `id` (and the vector field, see below) is sent as an object property, after `indexer.whitelist` and `indexer.blacklist` are applied. This includes `run_id` unless the filter excludes it. Child documents are not sent, either as a property or as separate objects.

WeaviateIndexer does not support `indexer.maxConcurrentBatches` greater than 1; setting it fails at startup.

---

## Deletion Not Supported

WeaviateIndexer logs a warning if `deletionMarkerField` or `deleteByFieldField` are configured but does not perform deletions. Documents marked for deletion are indexed as regular objects. This is a known limitation.

---

## sendEnabled and Test Mode

With `indexer.sendEnabled: false`, or when Lucille runs in test mode, no Weaviate client is created, connection validation passes, and batches are not sent; the documents are reported as indexed.

---

## Vector Field Handling

If `vectorField` is set and the document has that field, its values are read as floats and sent as the object's vector. Values are not checked: a non-numeric value is sent as `0.0`. The vector field is removed from the properties map to avoid storing it twice (once as a vector, once as a property).

If `vectorField` is omitted, no vector is sent. This is useful when Weaviate is configured to generate its own embeddings via vectorizer modules (e.g., `text2vec-openai`).

---

## Consistency Level

All writes use `ConsistencyLevel.ALL` (strongest consistency). This is not currently configurable. In a multi-node Weaviate cluster, this means all replicas must acknowledge the write before it is considered successful.

---

## Connection Timeouts

The Weaviate client is created with connection, connection request, and response timeouts of `6000` each, which the Weaviate client library interprets as seconds (100 minutes each). These are not configurable via the Lucille config.

---

## Errors

- If the batch request itself fails, the whole batch fails.
- If Weaviate reports an error for an individual object, only that document is marked as failed.
- These failures are not retryable, so `indexer.maxRetries` has no effect. The Weaviate client itself resends a batch up to 3 times after a connection error or timeout before reporting it as failed.

---

## Class/Schema Requirements

The `className` parameter determines which Weaviate class (schema type) objects are created under. The indexer does not create the class or its schema.

---

## Troubleshooting

**"Couldn't connect to Weaviate instance":**
The Weaviate client could not be created at startup. Verify `apiKey` and that the `host` value is correct (hostname only, no `https://` prefix).

**Connection validation fails:**
At startup the indexer requests Weaviate's meta endpoint. Errors from that request are logged and the run stops. Check the host and API key.

**Vector dimension mismatch:**
If sending vectors directly, ensure they match the class's configured vector dimensions.
