---
title: Pinecone Indexer
weight: 6
date: 2025-06-09
description: Configuration reference for the Pinecone Indexer, which indexes vector embeddings into Pinecone.
---

`com.kmwllc.lucille.pinecone.indexer.PineconeIndexer`

Indexes vector embeddings into [Pinecone](https://www.pinecone.io/). Config block: `pinecone { ... }`

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-pinecone</artifactId>
  <version>${lucille.version}</version>
</dependency>
```

| Parameter | Type | Required | Description |
|---|---|---|---|
| `apiKey` | String | Yes | Pinecone API key. Use `${PINECONE_API_KEY}` for environment variable substitution. |
| `index` | String | Yes | Name of the Pinecone index to write to. |
| `defaultEmbeddingField` | String | No | Document field containing the vector, used when `namespaces` is not set. Documents go to the `default` namespace. Required unless `namespaces` is set. |
| `namespaces` | Map\<String, String\> | No | Map of Pinecone namespace to the document field holding that namespace's vector. Must not be empty if set. |
| `metadataFields` | List\<String\> | No | Document fields sent as Pinecone metadata with each upserted vector. `indexer.whitelist` and `indexer.blacklist` are applied first. |
| `mode` | String | No | `upsert` or `update` (case-insensitive). Any other value fails at startup. Default: `upsert`. |

```hocon
indexer {
  class: "com.kmwllc.lucille.pinecone.indexer.PineconeIndexer"
  deletionMarkerField: "is_deleted"
  deletionMarkerFieldValue: "true"
}

pinecone {
  apiKey: ${PINECONE_API_KEY}
  index: "my-index"
  defaultEmbeddingField: "content_vector"
  metadataFields: ["title", "category"]
}
```

The vector field must hold a list of floats. In `upsert` mode a document without it fails the batch, so drop such documents earlier in the pipeline (for example with a `DropDocument` stage and a condition). Vector IDs are the Lucille document IDs, or the value of `indexer.idOverrideField` when it is set and present on the document; this applies to upserts, updates, and deletes. `indexer.indexOverrideField` is not supported and fails at startup; all documents go to `index` (use `namespaces` to split vectors). Child documents are not sent.

At startup, connection validation describes the index and fails if it does not exist or is initializing, failed to initialize, or terminating. PineconeIndexer does not support `indexer.maxConcurrentBatches` greater than 1; setting it fails at startup.

---

## Namespace Routing

When `namespaces` is configured, it is a map of namespace names to embedding field names. Each document is upserted into every configured namespace using the corresponding embedding field. This enables multi-vector indexing, for example, title embeddings in one namespace and body embeddings in another:

```hocon
pinecone {
  apiKey: ${PINECONE_API_KEY}
  index: "my-index"
  namespaces: {
    "title-ns": "title_vector"
    "body-ns": "body_vector"
  }
}
```

When `namespaces` is not set, all documents go to the namespace named `default` using `defaultEmbeddingField`.

---

## defaultEmbeddingField vs. namespaces

Two modes are available:

- **Single namespace**: Set `defaultEmbeddingField` to the name of the vector field. All documents are upserted into the `default` namespace.
- **Multi namespace**: Set `namespaces` as a map of namespace to embedding field. Documents are upserted into each namespace with the corresponding vector. When `namespaces` is set, `defaultEmbeddingField` is ignored.

At least one of these must be set when uploading documents. If both are omitted, startup succeeds but the first batch containing a document to upload fails.

---

## Metadata Fields

Only fields listed in `metadataFields` are sent as Pinecone metadata alongside the vector. All other document fields are ignored. Each value is sent as a string (`toString()` of the field value, so a multi-valued field becomes a string like `[a, b]`). A listed field whose value is null fails the batch. `indexer.whitelist` and `indexer.blacklist` are applied first, so a listed field they exclude is not sent. Metadata is only sent in `upsert` mode.

```hocon
pinecone {
  apiKey: ${PINECONE_API_KEY}
  index: "my-index"
  defaultEmbeddingField: "content_vector"
  metadataFields: ["title", "category", "source_url"]
}
```

---

## Upsert vs. Update Mode

| Mode | Behavior |
|---|---|
| `"upsert"` (default) | Creates or replaces vectors. If the ID exists, the vector and metadata are overwritten. |
| `"update"` | Updates the vector of existing records, one request per document. Metadata is not sent. Pinecone returns success if the ID does not exist, so missing records are not reported. |

```hocon
pinecone {
  mode: "update"
  // ...
}
```

---

## Batch Size Limit

Pinecone's API limits batches to 1000 vectors or 2MB, whichever is reached first. The indexer enforces the 1000-vector limit at startup: if `indexer.batchSize` exceeds 1000, the indexer throws an exception and refuses to start. Setting `indexer.batchByteSize` without `indexer.batchSize` also fails this check, because the document-count limit is then unbounded.

Higher-dimensional vectors may hit the 2MB limit at counts lower than 1000. If this happens, the Pinecone API returns an error at runtime. Reduce `indexer.batchSize` accordingly.

---

## Deletion

Deletion is by ID only (uses `deletionMarkerField` / `deletionMarkerFieldValue`). `deleteByFieldField` / `deleteByFieldValue` are ignored.

When `namespaces` is configured, deletion is issued to all configured namespaces; otherwise to the `default` namespace. Within a batch, if the same document ID is both uploaded and deleted, the last occurrence wins. Deletes are sent before uploads.

To delete every vector whose ID starts with a document's ID (for example, chunk vectors), use the [EmitDocsToDeleteByPrefix]({{< relref "docs/ingest-design/stages/emit_docs_to_delete_by_prefix" >}}) stage from the same plugin.

---

## Errors

- Any failed request (delete, upsert, or update) fails the whole batch. If a delete fails, that batch's uploads are not sent.
- An upsert whose reported count differs from the number of vectors sent also fails the batch.
- These failures are not retryable, so `indexer.maxRetries` has no effect.

---

## sendEnabled and Test Mode

With `indexer.sendEnabled: false`, or when Lucille runs in test mode, no Pinecone client is created, connection validation passes, and batches are not sent; the documents are reported as indexed. Config checks such as `mode` and `indexer.batchSize` still run at startup.

---

## Troubleshooting

**"Maximum batch size for Pinecone is 1000":**
Reduce `indexer.batchSize` to 1000 or lower.

**API key invalid:**
Verify `PINECONE_API_KEY` is set and the key has write access to the target index.

**Index not found:**
The Pinecone index must be created via the Pinecone console or API before running Lucille. Otherwise the run stops at startup.

**Vector dimension mismatch:**
The vector field on each document must have the same number of dimensions as the Pinecone index was created with. A mismatch causes a runtime error from the Pinecone API.
