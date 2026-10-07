---
title: EmitNestedChildren
weight: 300
date: 2025-06-09
description: Extracts a nested array from a Document and emits each element as an independent child Document.
---

`com.kmwllc.lucille.stage.EmitNestedChildren`

Extracts a nested array from a Document and emits each array element as an independent child Document.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `field` | String | Yes | Field containing the array of nested objects to extract. |
| `keepParent` | Boolean | No | Whether to also emit the parent Document. Default: `true`. |
