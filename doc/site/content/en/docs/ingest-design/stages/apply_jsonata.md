---
title: ApplyJSONata
weight: 90
date: 2025-06-09
description: Applies a JSONata expression to a field, or to the whole Document.
---

`com.kmwllc.lucille.stage.ApplyJSONata`

Applies a [JSONata](https://jsonata.org/) expression ([jsonata-java](https://github.com/dashjoin/jsonata-java) implementation).

- With `source`, the expression is evaluated on that field's value and the result is written to `destination`, or back to `source` if `destination` is not set. Documents without `source` are left unchanged.
- Without `source`, the expression is evaluated on the whole Document and the result replaces it. The result must be an object and must not change reserved fields such as `id`. This mode is experimental.

If the expression fails or returns no result on a Document, a warning is logged and the Document is left unchanged.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `expression` | String | Yes | JSONata expression to apply. Invalid expressions fail at startup. |
| `source` | String | No | Field to use as input. Default: the whole Document. |
| `destination` | String | No | Field for the result. Used only with `source`. Default: `source`. |
