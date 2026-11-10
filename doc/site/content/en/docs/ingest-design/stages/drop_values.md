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
- Values are compared as strings, so `values: ["5"]` also removes the number `5`. A field with no matching value is not changed. Otherwise the remaining values are written back with their original types, as a multi-valued field.
- If every value is removed, the field is removed.

```hocon
{
  class: "com.kmwllc.lucille.stage.DropValues"
  source: ["tags"]
  values: ["n/a", "unknown"]
}
```
