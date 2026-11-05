---
title: TruncateField
weight: 640
date: 2025-06-09
description: Truncates a field to a maximum number of characters, in place or into another field.
---

`com.kmwllc.lucille.stage.TruncateField`

Truncates each value of `source` to at most `maxSize` characters and writes the results to `destination`, overwriting it, or back to `source` if `destination` is not set. A single-valued field stays single-valued; null values are skipped. Documents without `source` are left unchanged.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | Yes | Field to truncate. |
| `maxSize` | Integer | Yes | Maximum number of characters. Must not be negative. |
| `destination` | String | No | Field for the truncated value. Default: truncate `source` in place. |
