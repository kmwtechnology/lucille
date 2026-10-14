---
title: SplitFieldValues
weight: 600
date: 2025-06-09
description: Splits a field value (or multi-valued field) by a delimiter into a list.
---

`com.kmwllc.lucille.stage.SplitFieldValues`

Splits a field value (or multi-valued field) by a delimiter into a list.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fields` | List\<String\> | Yes | Fields to split. |
| `delimiter` | String | No | Delimiter string. Default: `","`. |
| `updateMode` | String | No | `overwrite`, `append`, or `skip`. |
