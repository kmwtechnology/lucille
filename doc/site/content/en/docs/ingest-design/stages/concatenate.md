---
title: Concatenate
weight: 180
date: 2025-06-09
description: Concatenates the values of multiple source fields into a destination field.
---

`com.kmwllc.lucille.stage.Concatenate`

Concatenates the values of multiple source fields into a destination field.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | List\<String\> | Yes | Fields whose values will be concatenated. |
| `dest` | String | Yes | Destination field name. |
| `delimiter` | String | No | Separator inserted between values. Default: `""`. |
| `updateMode` | String | No | `overwrite`, `append`, or `skip`. |
