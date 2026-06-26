---
title: Weaviate Indexer
weight: 7
date: 2025-06-09
description: Configuration reference for the Weaviate Indexer — index documents and vectors into Weaviate.
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
| `apiKey` | String | Yes | Weaviate API key for authentication. Use `${?WEAVIATE_API_KEY}` for environment variable substitution. |
| `host` | String | Yes | Weaviate instance hostname (e.g., `my-cluster.weaviate.network`). Do not include the protocol — HTTPS is used automatically. |
| `className` | String | No | The Weaviate class (object type) to create or update. Default: `"Document"`. |
| `idDestinationName` | String | No | Field name under which the document's original Lucille ID is stored in Weaviate (since Weaviate's `id` field must be a UUID). Default: `"id_original"`. |
| `vectorField` | String | No | Document field containing the vector embedding to index. If omitted, no vector is sent (useful if Weaviate is configured to generate its own embeddings). |

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

**Note on IDs:** Weaviate requires UUIDs for its internal `id` field. The WeaviateIndexer generates a UUID from the document's Lucille ID and stores the original ID under `idDestinationName` so it can be retrieved later.

---

## UUID Generation

Weaviate requires UUIDs for its internal `id` field. The WeaviateIndexer generates a deterministic UUID from the document's Lucille ID using `UUID.nameUUIDFromBytes(id.getBytes())`. This means:

- The same Lucille ID always maps to the same Weaviate UUID, enabling idempotent upserts.
- The original Lucille ID is stored under `idDestinationName` (default: `"id_original"`) so it can be retrieved later.

---

## Deletion Not Supported

WeaviateIndexer logs a warning if `deletionMarkerField` or `deleteByFieldField` are configured but does not perform deletions. Documents marked for deletion are indexed as regular objects. This is a known limitation.

---

## Vector Field Handling

If `vectorField` is set and the document has that field, the vector is sent alongside the object properties. The vector field is removed from the properties map to avoid storing it twice (once as a vector, once as a property).

If `vectorField` is omitted, no vector is sent. This is useful when Weaviate is configured to generate its own embeddings via vectorizer modules (e.g., `text2vec-openai`).

---

## Consistency Level

All writes use `ConsistencyLevel.ALL` (strongest consistency). This is not currently configurable. In a multi-node Weaviate cluster, this means all replicas must acknowledge the write before it is considered successful.

---

## Connection Timeouts

The Weaviate client is configured with 6-second timeouts for connection, read, and write operations. These are not currently configurable via the Lucille config. If you experience timeout errors with large batches or slow networks, reduce `indexer.batchSize`.

---

## Class/Schema Requirements

The `className` parameter determines which Weaviate class (schema type) objects are created under. The class must already exist in the Weaviate schema — the indexer does not create it. If the class doesn't exist, the batch write will fail.

---

## Troubleshooting

**AuthException ("Couldn't connect to Weaviate instance"):**
The API key is invalid or the host is unreachable. Verify `WEAVIATE_API_KEY` and that the `host` value is correct (hostname only, no `https://` prefix).

**Class not found in schema:**
Create the Weaviate class via the Weaviate console or REST API before running Lucille.

**Vector dimension mismatch:**
If using a Weaviate vectorizer module, ensure the vector field dimensions match what the module expects. If sending vectors directly, ensure they match the class's configured vector dimensions.

**Timeout errors:**
Reduce `indexer.batchSize` to send smaller batches. The 6-second timeout is fixed.
