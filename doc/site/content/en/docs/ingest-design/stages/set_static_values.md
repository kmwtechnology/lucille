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
| `fields` | Map\<String, Object\> | Yes | Map of field name → static value to set. |
| `updateMode` | String | No | `overwrite`, `append`, or `skip`. Default: `overwrite`. |
| `skipDocument` | Boolean | No | If `true`, marks the document as skipped after setting values. The document bypasses downstream stages but still reaches the Indexer. Useful for combining field-setting and skipping in a single stage with one set of conditions. Default: `false`. |

```hocon
{ class: "com.kmwllc.lucille.stage.SetStaticValues", fields: { source: "my-connector", version: 2 } }
```
