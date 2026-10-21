---
title: Contains
weight: 190
date: 2025-06-09
description: Tags an output field with a value when any of the given fields contains any of the given terms.
---

`com.kmwllc.lucille.stage.Contains`

Checks whether any of `fields` contains any of the terms in `contains`. On the first match, it writes `value` to `output`, overwriting what is there. Terms match as whole words within a field value, not only as the entire value.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fields` | List\<String\> | Yes | Fields to search. |
| `contains` | List\<String\> | Yes | Terms to look for. Must not be empty. |
| `output` | String | Yes | Field to write to when a match is found. |
| `value` | String | Yes | Value to write to `output`. |
| `ignoreCase` | Boolean | No | Match case-insensitively. Default: `true`. |
