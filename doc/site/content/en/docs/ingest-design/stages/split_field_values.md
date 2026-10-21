---
title: SplitFieldValues
weight: 600
date: 2025-06-09
description: Splits the values of a field by a delimiter and adds the pieces to an output field.
---

`com.kmwllc.lucille.stage.SplitFieldValues`

Splits every value of `inputField` by `delimiter` and adds the pieces to `outputField`, which becomes multi-valued. Documents without `inputField` are skipped.

If `outputField` is the same as `inputField`, the original values are replaced by the pieces. Otherwise the pieces are appended to any existing values of `outputField`; there is no `updateMode`.

`delimiter` is a Java regular expression (it is passed to `String.split`), so characters such as `|` or `.` must be escaped. Trailing empty pieces are dropped.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `inputField` | String | Yes | Field to split. |
| `outputField` | String | Yes | Field to add the pieces to. Can be the same as `inputField`. |
| `delimiter` | String | Yes | Regular expression to split on. |
| `trimWhitespace` | Boolean | Yes | Trim leading and trailing whitespace from each piece. |

```hocon
{ class: "com.kmwllc.lucille.stage.SplitFieldValues", inputField: "tags", outputField: "tags", delimiter: ",", trimWhitespace: true }
```
