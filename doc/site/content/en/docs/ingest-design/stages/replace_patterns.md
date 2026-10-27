---
title: ReplacePatterns
weight: 570
date: 2025-06-09
description: Replaces all matches of one or more regular expressions in source fields and writes the results to destination fields.
---

`com.kmwllc.lucille.stage.ReplacePatterns`

For every value of each `source` field, replaces all matches of each pattern in `regex` with the replacement string, applying the patterns in order, and writes the results to the corresponding `dest` field. Every value is written, changed or not. Source fields missing from the Document are skipped. An invalid regex fails the stage at startup.

The replacement is the value of `replacementField` on the Document if that field is present, otherwise `replacement`. At least one of the two must be configured, or the stage fails at startup. A Document with neither a `replacementField` value nor a configured `replacement` is left unchanged. The replacement follows Java `Matcher.replaceAll` syntax, so `$1` refers to a capture group and a literal `$` or `\` must be escaped.

`source` and `dest` must have the same number of fields (1-1 mapping), or `dest` must have one field that receives the results from every `source` field.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | List\<String\> | Yes | Fields to process. |
| `dest` | List\<String\> | Yes | Destination fields. Same length as `source`, or a single field. With a single field, each source field's result is written to it in turn under `updateMode`, so use `append` to keep the results from every source. |
| `regex` | List\<String\> | Yes | Java regular expressions, applied in order. |
| `replacement` | String | No | Replacement string. Required unless `replacementField` is set. |
| `replacementField` | String | No | Field holding the replacement string for each Document. Takes precedence over `replacement` when present on the Document. Only the first value is used. |
| `ignoreCase` | Boolean | No | Case-insensitive matching (`Pattern.CASE_INSENSITIVE`). Default: `false`. |
| `multiline` | Boolean | No | `^` and `$` match at line boundaries (`Pattern.MULTILINE`). Default: `false`. |
| `dotall` | Boolean | No | `.` matches line terminators (`Pattern.DOTALL`). Default: `false`. |
| `literal` | Boolean | No | Treat each `regex` as a literal string (`Pattern.LITERAL`). Default: `false`. |
| `updateMode` | String | No | `overwrite`, `append`, or `skip`. Default: `overwrite`. |

```hocon
{ class: "com.kmwllc.lucille.stage.ReplacePatterns", source: ["body"], dest: ["body_redacted"], regex: ["\\d{3}-\\d{2}-\\d{4}"], replacement: "[REDACTED]" }
```
