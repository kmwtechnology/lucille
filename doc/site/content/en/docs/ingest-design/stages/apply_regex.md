---
title: ApplyRegex
weight: 130
date: 2025-06-09
description: Extracts all matches of a regular expression from source fields into destination fields.
---

`com.kmwllc.lucille.stage.ApplyRegex`

Applies a regular expression to every value of each `source` field and writes all matches to the corresponding `dest` field. If the regex has capture groups, the first group of each match is written; otherwise the whole match is written. Source fields missing from the Document, and fields with no matches, are skipped. An invalid regex fails the stage at startup.

`source` and `dest` must have the same number of fields (1-1 mapping), or `dest` must have one field that receives the results from every `source` field.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | List\<String\> | Yes | Fields to apply the regex to. |
| `dest` | List\<String\> | Yes | Destination fields. Same length as `source`, or a single field. With a single field, each source field's result is written to it in turn under `updateMode`, so use `append` to keep the results from every source. |
| `regex` | String | Yes | Java regular expression. |
| `ignoreCase` | Boolean | No | Case-insensitive matching (`Pattern.CASE_INSENSITIVE`). Default: `false`. |
| `multiline` | Boolean | No | `^` and `$` match at line boundaries (`Pattern.MULTILINE`). Default: `false`. |
| `dotall` | Boolean | No | `.` matches line terminators (`Pattern.DOTALL`). Default: `false`. |
| `literal` | Boolean | No | Treat `regex` as a literal string (`Pattern.LITERAL`). Default: `false`. |
| `updateMode` | String | No | `overwrite`, `append`, or `skip`. Default: `overwrite`. |

```hocon
{ class: "com.kmwllc.lucille.stage.ApplyRegex", source: ["body"], dest: ["phone_numbers"], regex: "\\(?\\d{3}\\)?[ -]?\\d{3}-\\d{4}" }
```
