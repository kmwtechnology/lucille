---
title: DropDocument
weight: 260
date: 2025-06-09
description: Marks a document as dropped so it is not sent to the Indexer.
---

`com.kmwllc.lucille.stage.DropDocument`

Marks a document as dropped. It will not be sent to the Indexer. By default every document is dropped; set `percentage` to drop documents at random.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `percentage` | Double | No | Probability, from `0.0` to `1.0` inclusive, that a document is dropped. Each document is decided independently. Values outside the range fail at startup. Default: `1.0`. |

Typically used with `conditions` to selectively drop documents:

```hocon
{
  class: "com.kmwllc.lucille.stage.DropDocument"
  conditions: [
    { fields: ["status"], values: ["deleted"] }
  ]
}
```
