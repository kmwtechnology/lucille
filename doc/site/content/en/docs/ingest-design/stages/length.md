---
title: Length
weight: 390
date: 2025-06-09
description: Counts the values in fields and writes each count to another field.
---

`com.kmwllc.lucille.stage.Length`

Counts the number of values in each source field and writes the count to its destination field, overwriting any existing value. A single-valued field counts as `1` and a missing field as `0`. It counts values, not characters.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fieldMapping` | Map\<String, String\> | Yes | Maps each field to measure to the field that receives its count. |

```hocon
{ class: "com.kmwllc.lucille.stage.Length", fieldMapping: { tags: "tags_count" } }
```
