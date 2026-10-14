---
title: AddRandomDouble
weight: 30
date: 2025-06-09
description: Adds a random double to a field.
---

`com.kmwllc.lucille.stage.AddRandomDouble`

Adds a random double to a field.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fieldName` | String | No | Destination field. Default: `data`. |
| `rangeStart` | Double | No | Minimum value (inclusive). Write it with a decimal point (`5.0`, not `5`); an integer value fails at startup. Default: `0.0`. |
| `rangeEnd` | Double | No | Maximum value (exclusive). Must be greater than `rangeStart`. Write it with a decimal point, like `rangeStart`. Default: `1000000.0`. |
