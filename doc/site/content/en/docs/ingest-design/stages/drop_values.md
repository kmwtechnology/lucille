---
title: DropValues
weight: 270
date: 2025-06-09
description: Removes values that exactly match a given list from the source fields.
---

`com.kmwllc.lucille.stage.DropValues`

Removes every value of the `source` fields that exactly matches one of `values`. Values that only contain a listed value are kept.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | List\<String\> | Yes | Fields to remove values from. Must not be empty. |
| `values` | List\<String\> | Yes | Values to remove. |

## Behavior

- Documents without a source field are not changed for that field.
- Values are compared as strings. The remaining values are written back as a multi-valued field of strings, even if the field was single-valued or held non-string values.
- If every value is removed, the field is removed.

```hocon
{
  class: "com.kmwllc.lucille.stage.DropValues"
  source: ["tags"]
  values: ["n/a", "unknown"]
}
```
