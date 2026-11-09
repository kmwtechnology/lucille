---
title: DropDocument
weight: 260
date: 2025-06-09
description: Marks a document as dropped so it is not sent to the Indexer.
---

`com.kmwllc.lucille.stage.DropDocument`

Marks a document as dropped. It will not be sent to the Indexer. By default every document is dropped; set `percentToDrop` to drop documents at random.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `percentToDrop` | Double | No | Percentage of documents to drop, from `0` to `100` inclusive. Each document is decided independently. Values outside the range fail at startup. Default: `100`. |

Typically used with `conditions` to selectively drop documents:

```hocon
{
  class: "com.kmwllc.lucille.stage.DropDocument"
  conditions: [
    { fields: ["status"], values: ["deleted"] }
  ]
}
```
