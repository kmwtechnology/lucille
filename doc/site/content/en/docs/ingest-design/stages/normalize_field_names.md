---
title: NormalizeFieldNames
weight: 410
date: 2025-06-09
description: Renames fields by replacing spaces with underscores and removing other non-alphanumeric characters.
---

`com.kmwllc.lucille.stage.NormalizeFieldNames`

Renames every non-reserved field on the document. Spaces are replaced with `_`, then every character other than an ASCII letter (`a`-`z`, `A`-`Z`), digit, `_`, or `.` is removed, so accented and non-Latin letters are dropped too. For example, `First Name!` becomes `First_Name`.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `delimiter` | String | No | Intended replacement for spaces. Currently ignored: the stage always uses `_`. |
| `nonAlphanumReplacement` | String | No | Intended replacement for non-alphanumeric characters. Currently ignored: the stage always removes them. |

If a normalized name matches a field that already exists, that field's value is overwritten. A name that normalizes to an empty string or to a reserved field name (such as `id`) fails the document.
