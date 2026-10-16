---
title: RemoveDiacritics
weight: 530
date: 2025-06-09
description: Removes accents and other diacritical marks from a String field.
---

`com.kmwllc.lucille.stage.RemoveDiacritics`

Removes diacritical marks from the value of `source` (for example, `é` becomes `e`). The text is decomposed with Unicode NFKD normalization and all combining marks are removed. NFKD also replaces compatibility characters, such as the ligature `ﬁ` with `fi`. Characters that do not decompose, such as `ø` or `ß`, are left unchanged, so the result is not necessarily ASCII.

Only the first value of a multi-valued `source` field is used, and the result is written as a single value, replacing any existing value. Documents without `source` are skipped.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | Yes | Field to process. |
| `destination` | String | No | Field for the result. Default: `source` (modified in place). |

```hocon
{ class: "com.kmwllc.lucille.stage.RemoveDiacritics", source: "name", destination: "name_ascii" }
```
