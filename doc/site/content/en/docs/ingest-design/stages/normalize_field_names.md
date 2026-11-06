---
title: NormalizeFieldNames
weight: 410
date: 2025-06-09
description: Renames fields by replacing spaces with underscores and removing other non-alphanumeric characters.
---

`com.kmwllc.lucille.stage.NormalizeFieldNames`

Renames every non-reserved field on the document. Spaces are replaced with `delimiter`, then every character other than an ASCII letter (`a`-`z`, `A`-`Z`), digit, `_`, or `.` is replaced with `nonAlphanumReplacement`, so accented and non-Latin letters are affected too. With the defaults, `First Name!` becomes `First_Name`.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `delimiter` | String | No | Replacement for spaces. Default: `_`. |
| `nonAlphanumReplacement` | String | No | Replacement for each non-alphanumeric character. Default: empty string (the character is removed). |

If a normalized name matches a field that already exists, that field's value is overwritten. A name that normalizes to an empty string or to a reserved field name (such as `id`) fails the document.
