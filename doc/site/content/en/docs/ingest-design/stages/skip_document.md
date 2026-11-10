---
title: SkipDocument
weight: 590
date: 2025-06-09
description: Marks a document as skipped so it bypasses downstream Stages but still reaches the Indexer.
---

`com.kmwllc.lucille.stage.SkipDocument`

Marks every document it processes as skipped. A skipped document bypasses all downstream Stages but still reaches the Indexer. Intended for deletes, where a document representing a delete should reach the Indexer without further processing. Usually combined with `conditions`.

This stage has no parameters beyond the shared stage parameters.

```hocon
{
  class: "com.kmwllc.lucille.stage.SkipDocument"
  conditions: [
    { fields: ["is_deleted"], values: ["true"] }
  ]
}
```
