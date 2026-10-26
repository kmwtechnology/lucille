---
title: ChunkText
weight: 150
date: 2025-06-09
description: Split a long text field into smaller, optionally overlapping chunks for embedding and RAG pipelines. Each chunk is attached to the Document as a child Document.
---

`com.kmwllc.lucille.stage.ChunkText`

The `ChunkText` Stage splits a long text field into smaller, optionally overlapping segments. Each segment is **attached** to the Document as a child Document. The children are not emitted on their own: they travel with the parent until a stage such as [EmitNestedChildren]({{< relref "docs/ingest-design/stages/emit_nested_children" >}}) emits them as independent Documents. The Solr indexer sends attached children as nested child documents of the parent; the OpenSearch and Elasticsearch indexers do so only when `childDocumentsField` is set, and otherwise do not index them. To index each chunk as its own top-level Document, which vector stores need, emit them with `EmitNestedChildren`. This is the foundation of retrieval-augmented generation (RAG) pipelines in Lucille.

Documents where `source` is missing, null, or blank are skipped. If `source` is multi-valued, only the first value is chunked.

## Configuration

```hocon
{
  class: "com.kmwllc.lucille.stage.ChunkText"
  source: "body"
  dest: "text"
  chunkingMethod: "sentence"
  chunksToMerge: 5
  chunksToOverlap: 1
  cleanChunks: true
}
```

## Configuration Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | Yes | Field containing the text to chunk. |
| `dest` | String | No | Field name for chunk content in child docs. Default: `text`. |
| `chunkingMethod` | String | No | Chunking strategy: `sentence`, `paragraph`, `fixed`, or `custom` (case-insensitive). Any other value silently falls back to `sentence`. Default: `sentence`. See below. |
| `regex` | String | No | Java regex to split on. Required when `chunkingMethod` is `custom`. Compiled when the stage is created, so a missing or invalid pattern fails at startup. |
| `lengthToSplit` | Integer | No | Number of characters per initial chunk. Required, and must be positive, when `chunkingMethod` is `fixed`. |
| `chunksToMerge` | Integer | No | How many initial chunks to merge into one final chunk, joined with a space. Must be at least `1`. Default: `1` (no merging). |
| `chunksToOverlap` | Integer | No | Number of initial chunks shared between consecutive final chunks. Merging then uses a sliding window of `chunksToMerge` chunks that advances by `chunksToMerge - chunksToOverlap`. Must be smaller than `chunksToMerge`. Cannot be combined with `overlapPercentage`. |
| `overlapPercentage` | Integer | No | After merging, adds characters from the end of the previous chunk and the start of the next chunk. The number of characters taken from each neighbour is this percentage of the current chunk's length. Cannot be combined with `chunksToOverlap`. Default: `0`. |
| `characterLimit` | Integer | No | Maximum character count for a final chunk, applied after merging and overlap. Longer chunks are truncated and trimmed. A value below `-1` fails at startup. |
| `preMergeMinChunkLen` | Integer | No | Before merging, consecutive initial chunks are joined until they reach this many characters. A short final remainder is appended to the previous chunk. Ignored if not positive. |
| `preMergeMaxChunkLen` | Integer | No | Initial chunks longer than this are truncated (and trimmed) before merging. Ignored if not positive. |
| `cleanChunks` | Boolean | No | Replace each line break, with its surrounding whitespace, by a single space and trim each initial chunk. Default: `false`. |

## Chunking Methods

### `sentence` (default)
Detects sentence boundaries using the bundled Apache OpenNLP English sentence model (`en-sent.bin`), loaded when the stage starts. This produces semantically coherent chunks. Best used with `chunksToMerge` to combine a few sentences into each final chunk.

```hocon
{
  class: "com.kmwllc.lucille.stage.ChunkText"
  source: "body"
  chunkingMethod: "sentence"
  chunksToMerge: 4      # 4 sentences per chunk
  chunksToOverlap: 1    # 1 sentence of overlap between chunks
}
```

### `paragraph`
Splits on two consecutive line breaks (`\n`, `\r`, or `\r\n`), with optional whitespace around them. Suitable for documents with paragraph structure.

```hocon
{
  class: "com.kmwllc.lucille.stage.ChunkText"
  source: "body"
  chunkingMethod: "paragraph"
  characterLimit: 1000    # Cap chunks at 1000 characters
}
```

### `fixed`
Splits every `lengthToSplit` characters. The last chunk may be shorter. Simple and predictable, but may cut mid-sentence.

```hocon
{
  class: "com.kmwllc.lucille.stage.ChunkText"
  source: "body"
  chunkingMethod: "fixed"
  lengthToSplit: 512
}
```

### `custom`
Splits on occurrences of a regex pattern. Useful when documents have a known structural delimiter.

```hocon
{
  class: "com.kmwllc.lucille.stage.ChunkText"
  source: "body"
  chunkingMethod: "custom"
  regex: "---+"    # Split on markdown horizontal rules
}
```

## Processing Order

The Stage applies transformations in this order:

1. **Initial chunking** by the chosen method.
2. **Cleaning** (if `cleanChunks: true`): replace line breaks with spaces, trim whitespace.
3. **Pre-merge processing**: join short chunks (`preMergeMinChunkLen`), then truncate long ones (`preMergeMaxChunkLen`).
4. **Merging and overlap**: with `chunksToOverlap`, merge using an overlapping sliding window. Otherwise merge every `chunksToMerge` chunks, then apply `overlapPercentage` if set.
5. **Character limiting** (if `characterLimit` set): truncate oversized final chunks.

## Child Document Fields

Each final chunk becomes an attached child document with these fields:

| Field | Description |
|---|---|
| `id` | `{parent_id}-{chunk_number}` (e.g., `doc-42-1`). |
| `parent_id` | ID of the parent document. |
| `chunk_number` | One-based index of this chunk. |
| `total_chunks` | Total number of chunks from this parent. |
| `offset` | Sum of the lengths of the preceding final chunks. This is not the position in the original text once cleaning, merging, or overlap changes the chunks. |
| `length` | Number of characters in the final chunk. |
| `{dest}` | The chunk text (default field name: `text`). |

All other fields from the parent document are **not** copied to child documents. `EmitNestedChildren` can copy parent fields with its `fieldsToCopy` option.

## Typical RAG Pipeline

```hocon
pipelines: [
  {
    name: "rag-pipeline"
    stages: [
      # 1. Extract text from PDF bytes
      {
        class: "com.kmwllc.lucille.tika.stage.TextExtractor"
        byteArrayField: "file_content"
        textField: "body"
      },
      # 2. Split into sentence-merged chunks
      {
        class: "com.kmwllc.lucille.stage.ChunkText"
        source: "body"
        dest: "chunk_text"
        chunkingMethod: "sentence"
        chunksToMerge: 5
        chunksToOverlap: 1
        cleanChunks: true
        characterLimit: 2000
      },
      # 3. Embed each chunk (only the attached children, which carry the chunk text)
      {
        class: "com.kmwllc.lucille.stage.OpenAIEmbed"
        source: "chunk_text"
        dest: "chunk_vector"
        embedDocument: false
        embedChildren: true
        apiKey: ${OPENAI_API_KEY}
      },
      # 4. Emit the chunks as independent Documents so each is indexed as its own record
      {
        class: "com.kmwllc.lucille.stage.EmitNestedChildren"
        dropParent: true
      }
    ]
  }
]
```

## Tips

- Use `embedDocument: false, embedChildren: true` in `OpenAIEmbed` to only embed chunks, not the full parent document.
- Set `characterLimit` to stay within your embedding model's input limit. The limit is in characters, not tokens.
- Use `cleanChunks: true` when the source text has formatting artifacts (embedded newlines from PDF extraction). Cleaning runs after splitting, so it does not change `paragraph` boundaries.
- `chunksToOverlap` and `chunksToMerge` work together: if you merge 5 sentences with 1 overlap, each chunk shares its last sentence with the next chunk, helping the model retrieve context that spans a chunk boundary. `chunksToOverlap` must be smaller than `chunksToMerge`.
