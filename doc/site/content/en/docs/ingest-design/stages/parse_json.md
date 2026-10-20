---
title: ParseJson
weight: 470
date: 2025-06-09
description: Parses a JSON string field and copies its fields, or values selected with JsonPath, onto the Document.
---

`com.kmwllc.lucille.stage.ParseJson`

Parses the JSON string in `src`. Without `jsonFieldPaths`, it copies every top-level field of the JSON object onto the Document; the JSON must then be an object. With `jsonFieldPaths`, it writes only the selected values. Documents without `src` are left unchanged. Invalid JSON fails the Document, as does writing to a reserved field name such as `id` (for example, a top-level `id` key in the JSON).

| Parameter | Type | Required | Description |
|---|---|---|---|
| `src` | String | Yes | Field containing the JSON string. |
| `jsonFieldPaths` | Map\<String, String\> | No | Maps each destination field to a [JsonPath](https://github.com/json-path/JsonPath) expression, such as `{title: "$.book.title"}`. Paths that match nothing are skipped. |
| `sourceIsBase64` | Boolean | No | `src` holds base64-encoded JSON, decoded before parsing. Default: `false`. |
| `updateMode` | String | No | `overwrite`, `append`, or `skip`. Default: `overwrite`. |
