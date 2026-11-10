---
title: RandomVector
weight: 520
date: 2025-06-09
description: Writes a random float vector to each of the given fields.
---

`com.kmwllc.lucille.stage.RandomVector`

Writes a new random float vector, with values from `-1` to `1`, to each field in `fields`. Useful for testing vector search pipelines.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fields` | List\<String\> | Yes | Fields to write a random vector to. |
| `dimensions` | Integer | Yes | Number of dimensions in each vector. |
| `updateMode` | String | No | `overwrite`, `append`, or `skip`. Default: `overwrite`. |
