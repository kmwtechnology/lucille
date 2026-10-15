---
title: JlamaEmbed
weight: 380
date: 2025-06-09
description: Generates vector embeddings using a quantized LLM running locally inside the JVM via Jlama.
---

`com.kmwllc.lucille.jlama.stage.JlamaEmbed` *(requires `lucille-jlama`)*

Generates vector embeddings using a quantized LLM running locally inside the JVM via [Jlama](https://github.com/tjake/Jlama). No API key or external service required — the model runs directly in the Lucille process. Useful for teams with data-residency or compliance constraints that prevent sending documents to external APIs.

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
  modelPath: "/models/my-embedding-model"
}
```
