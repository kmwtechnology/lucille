---
title: CopyFields
weight: 200
date: 2025-06-09
description: Copies source fields to one or more destination fields.
---

`com.kmwllc.lucille.stage.CopyFields`

Copies the values of each source field in `fieldMapping` to its destination field or fields. Source fields missing from a Document are skipped. Values are copied as strings, unless `isNested` is `true`.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fieldMapping` | Map\<String, String or List\<String\>\> | Yes | Maps each source field to a destination field, or to a list of destination fields. Must not be empty. |
| `updateMode` | String | No | `overwrite`, `append`, or `skip`. Ignored when `isNested` is `true`. Default: `overwrite`. |
| `isNested` | Boolean | No | Treat field names as dot-separated paths into nested JSON (such as `a.b.c`) and copy the JSON value as-is, always overwriting. Quote dotted keys in `fieldMapping` (`"a.b": "c.d"`); HOCON parses an unquoted dotted key as a nested object, which fails at startup. Default: `false`. |

```hocon
{ class: "com.kmwllc.lucille.stage.CopyFields", fieldMapping: { title: "title_copy", body: ["body_a", "body_b"] } }
```
