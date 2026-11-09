---
title: SplitFieldValues
weight: 600
date: 2025-06-09
description: Splits the values of a field by a delimiter and adds the pieces to an output field.
---

`com.kmwllc.lucille.stage.SplitFieldValues`

Splits every value of `source` by `delimiter` and adds the pieces to `dest`, which becomes multi-valued. Documents without `source` are skipped.

If `dest` is the same as `source`, the original values are replaced by the pieces. Otherwise the pieces are appended to any existing values of `dest`; there is no `updateMode`.

`delimiter` is a Java regular expression (it is passed to `String.split`), so characters such as `|` or `.` must be escaped. Trailing empty pieces are dropped. An invalid regular expression fails the stage at startup.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | Yes | Field to split. |
| `dest` | String | Yes | Field to add the pieces to. Can be the same as `source`. |
| `delimiter` | String | Yes | Regular expression to split on. |
| `trimWhitespace` | Boolean | Yes | Trim leading and trailing whitespace from each piece. |

```hocon
{ class: "com.kmwllc.lucille.stage.SplitFieldValues", source: "tags", dest: "tags", delimiter: ",", trimWhitespace: true }
```
