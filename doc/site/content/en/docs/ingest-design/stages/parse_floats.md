---
title: ParseFloats
weight: 460
date: 2025-06-09
description: Parses a JSON array string into a list of floats.
---

`com.kmwllc.lucille.stage.ParseFloats`

Parses a JSON array string (e.g., `"[0.1, 0.2, 0.3]"`) into a list of floats.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | Yes | Field containing the JSON array string. |
| `dest` | String | Yes | Destination field for the parsed list. |
