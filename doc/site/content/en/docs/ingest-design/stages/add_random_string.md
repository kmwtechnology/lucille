---
title: AddRandomString
weight: 60
date: 2025-06-09
description: Adds random terms to a field, either numbers or lines picked from a file.
---

`com.kmwllc.lucille.stage.AddRandomString`

Adds random terms to a field. Terms are either numbers from `0` to `rangeSize - 1` (as strings), or lines picked from the file at `inputDataPath`. The same term can be picked more than once for a document. Terms are added to any values already in `fieldName`, except with `isNested`, which overwrites the field.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fieldName` | String | No | Destination field. Default: `data`. |
| `inputDataPath` | String | No | Path to a local text file. Each line is a possible term. If omitted, terms are random numbers. |
| `rangeSize` | Integer | No | Without `inputDataPath`, the size of the number range. With `inputDataPath`, the number of lines to draw terms from (a random subset of the file); must not exceed the file's line count. Required if `inputDataPath` is not set. |
| `minNumOfTerms` | Integer | No | Minimum number of terms per document. Set together with `maxNumOfTerms`. Default: `1`. |
| `maxNumOfTerms` | Integer | No | Maximum number of terms per document. Set together with `minNumOfTerms`. Default: `1`. |
| `isNested` | Boolean | No | If `true`, writes the terms as a JSON array of objects, each holding a term under `data`. Default: `false`. |
| `concatenate` | Boolean | No | If `true`, joins the terms into one space-separated value instead of a multi-valued field. Cannot be combined with `isNested`. Default: `false`. |
