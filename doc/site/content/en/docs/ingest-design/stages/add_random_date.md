---
title: AddRandomDate
weight: 20
date: 2025-06-09
description: Adds a random date/timestamp to a field within a configured range.
---

`com.kmwllc.lucille.stage.AddRandomDate`

Adds a random date/timestamp to a field within a configured range.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fieldName` | String | No | Destination field. Default: `data`. |
| `rangeStartDate` | String | No | Start of the range (inclusive), as an ISO date such as `2024-12-18`, read as midnight UTC. Default: `1970-01-01`. |
| `rangeEndDate` | String | No | End of the range (exclusive), as an ISO date such as `2024-12-18`, read as midnight UTC. Must be after `rangeStartDate`; an equal value passes the startup check but then fails every document. Default: the current time. |
