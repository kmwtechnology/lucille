---
title: OpenAIEmbed
weight: 430
date: 2025-06-09
description: Generates vector embeddings for a text field using the OpenAI Embeddings API.
---

`com.kmwllc.lucille.stage.OpenAIEmbed`

Generates vector embeddings for a text field using the OpenAI Embeddings API.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | **Yes** | Field containing the text to embed. |
| `apiKey` | String | **Yes** | OpenAI API key. Use `${OPENAI_API_KEY}` for environment variable substitution. |
| `embedDocument` | Boolean | **Yes** | Whether to embed the main document. |
| `embedChildren` | Boolean | **Yes** | Whether to embed child documents. |
| `dest` | String | No | Field to write the embedding vector to. Default: `embeddings`. |
| `modelName` | String | No | OpenAI embedding model. Default: `text-embedding-3-small`. |
| `dimensions` | Integer | No | Output vector dimensions (only supported by `text-embedding-3-*` models). |

**Supported models:** `text-embedding-3-small`, `text-embedding-3-large`, `text-embedding-ada-002`

Text is truncated to 8,191 tokens before embedding (the OpenAI API limit). Lucille uses [jtokkit](https://github.com/knuddelsgmbh/jtokkit) for accurate token counting before the API call.

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
