---
title: ChunkText
date: 2025-06-09
description: Split a long text field into smaller overlapping chunks for embedding and RAG pipelines. Each chunk becomes a child document.
---

The `ChunkText` Stage splits a long text field into smaller, optionally overlapping segments. Each segment is emitted as a **child document** that flows independently through all downstream stages and is indexed as its own record. This is the foundation of retrieval-augmented generation (RAG) pipelines in Lucille.

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
| `source` | String | **Yes** | Field containing the text to chunk. |
| `dest` | String | No | Field name for chunk content in child docs. Default: `text`. |
| `chunkingMethod` | String | No | Chunking strategy. Default: `sentence`. See below. |
| `regex` | String | Required for `custom` | Regex pattern to split on. |
| `lengthToSplit` | Integer | Required for `fixed` | Number of characters per chunk. |
| `chunksToMerge` | Integer | No | How many initial chunks to merge into one final chunk. Default: `1` (no merging). |
| `chunksToOverlap` | Integer | No | Number of chunks from the previous final chunk to prepend to the current one. |
| `overlapPercentage` | Integer | No | Percentage of the current chunk's characters to add from its neighbours. Default: `0`. |
| `characterLimit` | Integer | No | Hard maximum character count for a final chunk after all merging. |
| `preMergeMinChunkLen` | Integer | No | Initial chunks shorter than this are appended to a neighbour before merging. |
| `preMergeMaxChunkLen` | Integer | No | Initial chunks longer than this are truncated before merging. |
| `cleanChunks` | Boolean | No | Remove internal newlines and trim whitespace from each chunk. Default: `false`. |

## Chunking Methods

### `sentence` (default)
Detects sentence boundaries using an Apache OpenNLP sentence detector. This produces semantically coherent chunks. Best used with `chunksToMerge` to combine a few sentences into each final chunk.

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
Splits on consecutive line breaks (`\n\n`, `\r\n\r\n`, etc.). Suitable for documents with paragraph structure.

```hocon
{
  class: "com.kmwllc.lucille.stage.ChunkText"
  source: "body"
  chunkingMethod: "paragraph"
  characterLimit: 1000    # Cap chunks at 1000 characters
}
```

### `fixed`
Splits every `lengthToSplit` characters. Simple and predictable, but may cut mid-sentence.

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
2. **Cleaning** (if `cleanChunks: true`): strip newlines, trim whitespace.
3. **Pre-merge filtering**: drop or truncate chunks based on `preMergeMinChunkLen` / `preMergeMaxChunkLen`.
4. **Merging** (if `chunksToMerge > 1`): combine N chunks into each final chunk.
5. **Overlap** (if `chunksToOverlap` or `overlapPercentage` set): prepend content from adjacent chunks.
6. **Character limiting** (if `characterLimit` set): truncate oversized final chunks.

## Child Document Fields

Each chunk becomes a child document with these fields:

| Field | Description |
|---|---|
| `id` | `{parent_id}-chunk-{n}` (e.g., `doc-42-chunk-0`). |
| `parent_id` | ID of the parent document. |
| `chunk_number` | Zero-based index of this chunk. |
| `total_chunks` | Total number of chunks from this parent. |
| `offset` | Character offset of the chunk's start in the original text. |
| `length` | Number of characters in the chunk. |
| `{dest}` | The chunk text (default field name: `text`). |

All other fields from the parent document are **not** copied to child documents unless a downstream Stage does so explicitly.

## Typical RAG Pipeline

```hocon
pipelines: [
  {
    name: "rag-pipeline"
    stages: [
      # 1. Extract text from PDF bytes
      {
        class: "com.kmwllc.lucille.tika.stage.TextExtractor"
        source: "file_content"
        dest: "body"
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
      # 3. Embed each chunk (only child docs, which carry the chunk text)
      {
        class: "com.kmwllc.lucille.stage.OpenAIEmbed"
        source: "chunk_text"
        dest: "chunk_vector"
        embedDocument: false
        embedChildren: true
        apiKey: ${OPENAI_API_KEY}
      }
    ]
  }
]
```

## Tips

- Use `embedDocument: false, embedChildren: true` in `OpenAIEmbed` to only embed chunks, not the full parent document.
- Set `characterLimit` to stay within your embedding model's token limit. For `text-embedding-3-small`, 8,191 tokens ≈ roughly 6,000–7,000 characters of English text.
- Use `cleanChunks: true` when the source text has formatting artifacts (extra whitespace, embedded newlines from PDF extraction).
- `chunksToOverlap` and `chunksToMerge` work together: if you merge 5 sentences with 1 overlap, each chunk shares its last sentence with the next chunk, helping the model retrieve context that spans a chunk boundary.
