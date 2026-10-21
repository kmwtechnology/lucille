---
title: NormalizeText
weight: 420
date: 2025-06-09
description: Converts text fields to lowercase, uppercase, title case, or sentence case.
---

`com.kmwllc.lucille.stage.NormalizeText`

Converts every value of each `source` field to the case given by `mode` and writes the results to the corresponding `dest` field. Source fields missing from the Document are skipped.

`source` and `dest` must have the same number of fields (1-1 mapping), or `dest` must have one field that receives the results from every `source` field.

Modes (case-insensitive; any other value fails the stage at startup):
- `lowercase`: all characters lowercased.
- `uppercase`: all characters uppercased.
- `title_case`: the first letter of each whitespace-separated word is uppercased and the rest lowercased, so acronyms are not preserved.
- `sentence_case`: the text is lowercased, then the first character and the first letter after each `.`, `?`, or `!` followed by a space are uppercased. Proper nouns and acronyms are not preserved. An empty value fails the document.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | List\<String\> | Yes | Fields to normalize. |
| `dest` | List\<String\> | Yes | Destination fields. Same length as `source`, or a single field. With a single field, each source field's result is written to it in turn under `updateMode`, so use `append` to keep the results from every source. |
| `mode` | String | Yes | `lowercase`, `uppercase`, `title_case`, or `sentence_case`. |
| `updateMode` | String | No | `overwrite`, `append`, or `skip`. Default: `overwrite`. |

```hocon
{ class: "com.kmwllc.lucille.stage.NormalizeText", source: ["title"], dest: ["title_display"], mode: "title_case" }
```
