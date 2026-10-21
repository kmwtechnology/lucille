---
title: JlamaEmbed
weight: 380
date: 2025-06-09
description: Generates vector embeddings with a HuggingFace embedding model running locally inside the JVM via Jlama.
---

`com.kmwllc.lucille.jlama.stage.JlamaEmbed` *(requires `lucille-jlama`)*

Generates vector embeddings with an embedding model running locally inside the JVM via [Jlama](https://github.com/tjake/Jlama). No API key or external service is needed to embed documents; the model runs in the Lucille process. The model must be in safetensors format and use a BERT architecture; other model types are not supported.

At startup the stage downloads `model` from HuggingFace into `pathToStoreModel`, unless it is already there, and loads it. The stage fails at startup if the model cannot be downloaded or loaded. Jlama uses the incubating Java Vector API, so run Lucille with `--add-modules jdk.incubator.vector` (for example, `export JDK_JAVA_OPTIONS='--add-modules jdk.incubator.vector'`).

For each document, the first value of `source` is embedded and each float of the embedding is added to `dest` as a separate value. If `dest` already has values, the embedding is appended to them. Documents where `source` is missing or blank are skipped with a warning. An error while embedding fails the document.

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-jlama</artifactId>
  <version>${lucille.version}</version>
</dependency>
```

```hocon
{
  name: "embed"
  class: "com.kmwllc.lucille.jlama.stage.JlamaEmbed"
  source: "content"
  dest: "content_vector"
  model: "intfloat/e5-small-v2"
  pathToStoreModel: "/models"
}
```

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | Yes | Field containing the text to embed. |
| `model` | String | Yes | HuggingFace model name in the form `owner/name`. |
| `pathToStoreModel` | String | Yes | Directory the model is downloaded to and loaded from. |
| `dest` | String | No | Field the embedding is written to. Default: `embeddings`. |
| `workingMemoryType` | String | No | Data type used for the model at runtime. Affects accuracy and resource use. One of `BOOL`, `U8`, `I8`, `I16`, `U16`, `F16`, `BF16`, `I32`, `U32`, `F32`, `F64`, `I64`, `U64`, `Q4`, or `Q5` (case-insensitive). An unrecognized value falls back to the default. Default: `F32`. |
| `workingQuantizationType` | String | No | Data type used for quantization. Trades accuracy for speed and size. Same values as `workingMemoryType`. Default: `I8`. |
| `deleteModelAfter` | Boolean | No | Delete the downloaded model directory when the stage stops. Default: `false`. |
