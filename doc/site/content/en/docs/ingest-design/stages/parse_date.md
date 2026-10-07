---
title: ParseDate
weight: 440
date: 2025-06-09
description: Parses date strings using configurable format patterns.
---

`com.kmwllc.lucille.stage.ParseDate`

Parses date strings using configurable format patterns and writes an `Instant` or formatted string.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | List\<String\> | Yes | Fields containing date strings. |
| `dest` | List\<String\> | No | Destination fields. Defaults to overwriting source. |
| `formats` | List\<String\> | Yes | Date format patterns to try, in order. |
| `timezone` | String | No | Timezone for parsing. Default: UTC. |
