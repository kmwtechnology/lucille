---
title: TruncateField
weight: 640
date: 2025-06-09
description: Truncates a field to a maximum number of characters, in place or into another field.
---

`com.kmwllc.lucille.stage.TruncateField`

Truncates the value of `source` to at most `maxSize` characters and writes it to `destination`, overwriting it, or back to `source` if `destination` is not set. Only the first value of a multi-valued field is used, and the result is written as a single value, so truncating in place drops the other values. Documents without `source` are left unchanged.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | Yes | Field to truncate. |
| `maxSize` | Integer | Yes | Maximum number of characters. Must not be negative. |
| `destination` | String | No | Field for the truncated value. Default: truncate `source` in place. |
