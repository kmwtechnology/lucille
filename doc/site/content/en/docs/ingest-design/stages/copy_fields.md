---
title: CopyFields
weight: 200
date: 2025-06-09
description: Copies one or more source fields to destination fields.
---

`com.kmwllc.lucille.stage.CopyFields`

Copies one or more source fields to destination fields.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | List\<String\> | Yes | Source field names. |
| `dest` | List\<String\> | Yes | Destination field names (parallel to `source`). |
| `updateMode` | String | No | `overwrite`, `append`, or `skip`. Default: `overwrite`. |

```hocon
{ class: "com.kmwllc.lucille.stage.CopyFields", source: ["title"], dest: ["title_copy"] }
```
