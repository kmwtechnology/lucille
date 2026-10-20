---
title: OpenAIEmbed
weight: 430
date: 2025-06-09
description: Generates vector embeddings for a text field using the OpenAI Embeddings API.
---

`com.kmwllc.lucille.stage.OpenAIEmbed`

Generates vector embeddings for the `source` field of a Document, its children, or both, using the OpenAI Embeddings API. The Document and its children are embedded together in one batch call. Only the first value of a multi-valued `source` field is embedded. A Document or child whose `source` is missing, null, or blank is not embedded.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | Yes | Field containing the text to embed. |
| `apiKey` | String | Yes | OpenAI API key. Use `${OPENAI_API_KEY}` for environment variable substitution. Must not be blank. |
| `embedDocument` | Boolean | Yes | Whether to embed the Document itself. |
| `embedChildren` | Boolean | Yes | Whether to embed the Document's children. At least one of `embedDocument` and `embedChildren` must be `true`. |
| `dest` | String | No | Field to write the embedding to. Each vector component is added as a separate Float value, so the field holds a list of floats. If `dest` already has values, the vector is appended to them. Default: `embeddings`. |
| `modelName` | String | No | `text-embedding-3-small`, `text-embedding-3-large`, or `text-embedding-ada-002`. An unrecognized name falls back to `text-embedding-3-small`. Default: `text-embedding-3-small`. |
| `dimensions` | Integer | No | Output vector dimensions, passed to the OpenAI API (only `text-embedding-3-*` models support it). If omitted, the model's default is used. |

Text is truncated to 8,191 tokens before embedding (the OpenAI API limit). Lucille uses [jtokkit](https://github.com/knuddelsgmbh/jtokkit) to count tokens with the encoding for the configured model.

The stage fails at startup if both `embedDocument` and `embedChildren` are `false`, or if `apiKey` is blank. A failed API request throws an exception for that Document.

When `embedChildren` is `true`, the Document's children are replaced with the embedded copies. Children that were not embedded (because their `source` is missing or blank) are removed from the Document.

```hocon
{
  class: "com.kmwllc.lucille.stage.OpenAIEmbed"
  source: "content"
  dest: "content_vector"
  modelName: "text-embedding-3-small"
  apiKey: ${OPENAI_API_KEY}
  embedDocument: true
  embedChildren: true
}
```
