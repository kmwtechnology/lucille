---
title: ApplyRegex
weight: 130
date: 2025-06-09
description: Applies a regular expression to one or more fields to extract groups or check for matches.
---

`com.kmwllc.lucille.stage.ApplyRegex`

Applies a regular expression to one or more fields. Can extract capture groups or check for matches.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | List\<String\> | Yes | Fields to apply the regex to. |
| `dest` | List\<String\> | No | Fields to write extracted groups to (parallel to `source`). |
| `regex` | String | Yes | The regular expression pattern. |
| `updateMode` | String | No | `overwrite`, `append`, or `skip`. |
