---
title: AddRandomInt
weight: 40
date: 2025-06-09
description: Adds a random integer to a field.
---

`com.kmwllc.lucille.stage.AddRandomInt`

Adds a random integer to a field.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fieldName` | String | No | Destination field. Default: `data`. |
| `rangeStart` | Integer | No | Minimum value (inclusive). Default: `0`. |
| `rangeEnd` | Integer | No | Maximum value (exclusive). Must be greater than `rangeStart`; an equal value passes the startup check but then fails every document. Default: `1000000`. |
