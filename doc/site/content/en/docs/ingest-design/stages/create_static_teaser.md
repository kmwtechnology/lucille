---
title: CreateStaticTeaser
weight: 220
date: 2025-06-09
description: Creates a teaser of at most a given number of characters from text fields.
---

`com.kmwllc.lucille.stage.CreateStaticTeaser`

Creates a teaser of at most `maxLength` characters from each source field. Text shorter than `maxLength` is copied whole (trimmed). Text of `maxLength` characters or more is cut back to the last space or `.`, `?`, `!`, `,` before the limit, so the teaser ends before a partial word. If there is no such break, the text is cut at exactly `maxLength`. Only the first value of a multi-valued field is used.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | List\<String\> | Yes | Source text fields. |
| `dest` | List\<String\> | Yes | Destination fields. Give one per `source` field, or a single field for all of them. With a single field, each source field's result is written to it in turn under `updateMode`, so use `append` to keep the results from every source. |
| `maxLength` | Integer | Yes | Maximum teaser length in characters. Must be at least `1`. |
| `updateMode` | String | No | `overwrite`, `append`, or `skip`. Default: `overwrite`. |
