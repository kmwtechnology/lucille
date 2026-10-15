---
title: ApplyJSONata
weight: 90
date: 2025-06-09
description: Applies a JSONata expression to transform the document's JSON representation.
---

`com.kmwllc.lucille.stage.ApplyJSONata`

Applies a [JSONata](https://jsonata.org/) expression to transform the document's JSON representation.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `expression` | String | Yes | JSONata expression to apply. |
| `dest` | String | No | Destination field for the expression result. If omitted, results are merged onto the document. |
