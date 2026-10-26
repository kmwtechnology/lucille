---
title: EmitDocsToDeleteByPrefix
weight: 295
date: 2025-06-09
description: Emits a deletion Document for every Pinecone vector whose ID starts with the ID of a Document marked for deletion.
---

`com.kmwllc.lucille.pinecone.stage.EmitDocsToDeleteByPrefix` *(requires `lucille-pinecone`)*

For each Document marked for deletion (`deletionMarkerField` equals `deletionMarkerFieldValue`), lists the vectors in the Pinecone index whose ID starts with the Document's ID followed by `addPrefix`, and emits one new Document per matching vector ID. Each emitted Document has only its ID and `deletionMarkerField` set to `deletionMarkerFieldValue`, so the [Pinecone Indexer]({{< relref "docs/ingest-design/indexers/pinecone_indexer" >}}) deletes it. Use this when one source record is indexed as several vectors (for example, chunks with IDs like `doc1-0`, `doc1-1`).

Documents not marked for deletion pass through unchanged.

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
| `apiKey` | String | Yes | Pinecone API key. |
| `index` | String | Yes | Name of the Pinecone index to list vectors from. |
| `deletionMarkerField` | String | Yes | Field that marks a Document for deletion. Set it to the same value as `indexer.deletionMarkerField`. |
| `deletionMarkerFieldValue` | String | Yes | Value of `deletionMarkerField` that marks a Document for deletion. Set it to the same value as `indexer.deletionMarkerFieldValue`. |
| `addPrefix` | String | No | String appended to the Document ID to form the prefix. With ID `doc1` and `addPrefix: "-"`, vectors starting with `doc1-` are matched. Default: `""`. |
| `namespaces` | Map\<String, String\> | No | Namespaces to list vectors from. Only the keys are used, so the same map as `pinecone.namespaces` can be reused. If not set, the `default` namespace is used. |
| `dropOriginal` | Boolean | No | Drop the original Document after listing, so it is not sent to the Indexer. Emitted Documents are not dropped. Default: `false`. |

```hocon
{
  name: "emitDeletes"
  class: "com.kmwllc.lucille.pinecone.stage.EmitDocsToDeleteByPrefix"
  apiKey: ${PINECONE_API_KEY}
  index: "my-index"
  deletionMarkerField: "is_deleted"
  deletionMarkerFieldValue: "true"
  addPrefix: "-"
  namespaces: {
    "title-ns": "title_vector"
    "body-ns": "body_vector"
  }
  dropOriginal: true
}
```

## Behavior

- At startup the stage connects to Pinecone and fails if the index cannot be reached or is initializing, failed to initialize, or terminating.
- The prefix match uses the Document ID, not `indexer.idOverrideField`.
- The match is a plain string prefix. With the default empty `addPrefix`, ID `doc1` also matches `doc1` itself and vectors such as `doc10`.
- With several namespaces, an ID found in more than one namespace is emitted once per namespace. The Pinecone Indexer deletes each emitted ID from every configured namespace.
- If `dropOriginal` is `false`, the original Document is still marked for deletion, so the Indexer also deletes the vector with exactly that ID.
- An error from the Pinecone list call fails the Document.
