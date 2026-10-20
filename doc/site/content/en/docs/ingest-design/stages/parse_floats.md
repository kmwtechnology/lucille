---
title: ParseFloats
weight: 460
date: 2025-06-09
description: Parses a JSON array string into a multi-valued field of floats.
---

`com.kmwllc.lucille.stage.ParseFloats`

Parses a JSON array string (e.g., `"[0.1, 0.2, 0.3]"`) into a multi-valued field of floats.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `field` | String | Yes | Field containing the JSON array string. If the field is multi-valued, only the first value is parsed. |
| `dest` | String | No | Destination field for the floats. Any existing value is replaced. If not set, `field` is overwritten. |

## Behavior

- Documents without `field` are not changed.
- If the string cannot be parsed as an array of numbers, a warning is logged and the document is not changed.
- If the array is empty, the destination field is removed.
