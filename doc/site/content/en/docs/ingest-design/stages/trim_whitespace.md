---
title: TrimWhitespace
weight: 630
date: 2025-06-09
description: Trims leading and trailing whitespace from every value of the given fields.
---

`com.kmwllc.lucille.stage.TrimWhitespace`

Trims leading and trailing whitespace from every value of the given fields, using Java `String.trim()`. Fields missing from the Document are skipped. Values are rewritten as strings, so non-string values (numbers, booleans) become strings, and a multi-valued field holding a single value becomes single-valued.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fields` | List\<String\> | Yes | Fields to trim. Must contain at least one field, or the stage fails at startup. |

```hocon
{ class: "com.kmwllc.lucille.stage.TrimWhitespace", fields: ["title", "author"] }
```
