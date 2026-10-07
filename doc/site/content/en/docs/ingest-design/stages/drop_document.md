---
title: DropDocument
weight: 260
date: 2025-06-09
description: Marks a document as dropped so it is not sent to the Indexer.
---

`com.kmwllc.lucille.stage.DropDocument`

Marks a document as dropped. It will not be sent to the Indexer.

Typically used with `conditions` to selectively drop documents:

```hocon
{
  class: "com.kmwllc.lucille.stage.DropDocument"
  conditions: [
    { fields: ["status"], values: ["deleted"] }
  ]
}
```
