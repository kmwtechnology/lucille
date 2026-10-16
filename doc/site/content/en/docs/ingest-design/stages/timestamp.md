---
title: Timestamp
weight: 620
date: 2025-06-09
description: Writes the current time to a field as an ISO-8601 instant.
---

`com.kmwllc.lucille.stage.Timestamp`

Writes the current time to `destField` as an ISO-8601 instant string, such as `2024-01-15T09:30:00.123Z`, overwriting any existing value.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `destField` | String | Yes | Field to write the timestamp to. |
