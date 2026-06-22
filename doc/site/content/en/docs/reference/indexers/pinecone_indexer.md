---
title: Pinecone Indexer
weight: 6
date: 2025-06-09
description: Configuration reference for the Pinecone Indexer — index vector embeddings into Pinecone.
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
| `vectorField` | String | Yes | Document field containing the vector embedding. |
| `namespace` | String | No | Pinecone namespace. Default: `"default"`. |

```hocon
indexer {
  class: "com.kmwllc.lucille.pinecone.indexer.PineconeIndexer"
  deletionMarkerField: "is_deleted"
  deletionMarkerFieldValue: "true"
}

pinecone {
  apiKey: ${PINECONE_API_KEY}
  index: "my-index"
  vectorField: "content_vector"
  namespace: "default"
}
```

---

## Namespace Routing

When `namespaces` is configured, it is a map of namespace names to embedding field names. Each document is upserted into every configured namespace using the corresponding embedding field. This enables multi-vector indexing — for example, title embeddings in one namespace and body embeddings in another:

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

When `namespaces` is not set, all documents go to the default namespace using `defaultEmbeddingField`.

---

## defaultEmbeddingField vs. namespaces

Two modes are available:

- **Single namespace** — Set `defaultEmbeddingField` to the name of the vector field. All documents are upserted into the default namespace.
- **Multi namespace** — Set `namespaces` as a map of namespace → embedding field. Documents are upserted into each namespace with the corresponding vector.

At least one of these must be set when uploading documents. If both are omitted, the indexer throws at upload time.

---

## Metadata Fields

Only fields listed in `metadataFields` are sent as Pinecone metadata alongside the vector. All other document fields are ignored. Metadata values are converted to strings.

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
| `"update"` | Updates existing vectors in place. Does not create new records. Silently succeeds (HTTP 200) if the ID doesn't exist. |

```hocon
pinecone {
  mode: "update"
  // ...
}
```

---

## Batch Size Limit

Pinecone's API limits batches to 1000 vectors or 2MB, whichever is reached first. The indexer enforces the 1000-vector limit at startup — if `indexer.batchSize` exceeds 1000, the indexer throws an exception and refuses to start.

Higher-dimensional vectors may hit the 2MB limit at counts lower than 1000. If this happens, the Pinecone API returns an error at runtime. Reduce `indexer.batchSize` accordingly.

---

## Deletion

Deletion is by ID only (uses `deletionMarkerField` / `deletionMarkerFieldValue`). Delete-by-query is not supported.

When `namespaces` is configured, deletion is issued to all configured namespaces.

---

## Troubleshooting

**"Maximum batch size for Pinecone is 1000":**
Reduce `indexer.batchSize` to 1000 or lower.

**API key invalid:**
Verify `PINECONE_API_KEY` is set and the key has write access to the target index.

**Index not found:**
The Pinecone index must be created via the Pinecone console or API before running Lucille.

**Vector dimension mismatch:**
The vector field on each document must have the same number of dimensions as the Pinecone index was created with. A mismatch causes a runtime error from the Pinecone API.
