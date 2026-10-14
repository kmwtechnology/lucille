---
title: SkipDocument
weight: 590
date: 2025-06-09
description: Marks a document as skipped so it bypasses downstream Stages but still reaches the Indexer.
---

`com.kmwllc.lucille.stage.SkipDocument`

Marks a document as skipped. It bypasses all downstream Stages but still reaches the Indexer. Used to issue deletes against a search backend.

```hocon
{
  class: "com.kmwllc.lucille.stage.SkipDocument"
  conditions: [
    { fields: ["is_deleted"], values: ["true"] }
  ]
}
```
