---
title: Contains
weight: 190
date: 2025-06-09
description: Checks whether a field's value is contained in a configured list and sets a boolean result field.
---

`com.kmwllc.lucille.stage.Contains`

Checks whether a field's value is contained in a configured list. Sets a boolean result field.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `field` | String | Yes | Field to check. |
| `values` | List\<String\> | Yes | Values to look for. |
| `dest` | String | Yes | Destination boolean field. |
