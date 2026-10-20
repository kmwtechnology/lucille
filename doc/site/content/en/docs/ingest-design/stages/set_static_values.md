---
title: SetStaticValues
weight: 580
date: 2025-06-09
description: Sets one or more fields to fixed, static values.
---

`com.kmwllc.lucille.stage.SetStaticValues`

Sets one or more fields to fixed, static values.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `staticValues` | Map\<String, Object\> | Yes | Map of field name to the static value to set. Values must be strings, numbers, or booleans; a list or object value makes processing fail with an exception. |
| `updateMode` | String | No | `overwrite`, `append`, or `skip`. An unrecognized value falls back to `overwrite`. Default: `overwrite`. |
| `skipDocument` | Boolean | No | If `true`, marks the document as skipped after setting values, whether or not any field changed. The document bypasses downstream stages but still reaches the Indexer. Useful for combining field-setting and skipping in a single stage with one set of conditions. Default: `false`. |

```hocon
{ class: "com.kmwllc.lucille.stage.SetStaticValues", staticValues: { source: "my-connector", version: 2 } }
```
