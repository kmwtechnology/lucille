---
title: ParseDate
weight: 440
date: 2025-06-09
description: Parses date strings with format patterns or formatter classes and writes them as ISO-8601 instants.
---

`com.kmwllc.lucille.stage.ParseDate`

Parses the values in `source` and writes each as an ISO-8601 instant string, such as `2024-01-15T00:00:00Z`. For each value, the patterns in `formatStrs` are tried in order, then the classes in `formatters`. Values that nothing can parse are left out of the output. If `dest` is the same field as `source` and no value parses, the field is removed.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | List\<String\> | Yes | Fields containing date strings. |
| `dest` | List\<String\> | Yes | Destination fields. Give one per `source` field, or a single field for all of them. With a single field, each source field's result is written to it in turn under `updateMode`, so use `append` to keep the results from every source. |
| `formatStrs` | List\<String\> | No | `DateTimeFormatter` patterns to try, in order. A pattern only has to match the start of a value; trailing text is ignored. A missing hour defaults to midnight. |
| `formatters` | List\<Object\> | No | Formatter classes to try after `formatStrs`, each given as `{class: "..."}`. A class must implement `BiFunction<String, ZoneId, ZonedDateTime>` and have a no-argument constructor. Built-in formatters are in `com.kmwllc.lucille.stage.dateformatters`. |
| `timeZoneId` | String | No | Time zone used when parsing, such as `America/New_York`. Default: the system time zone. |
| `updateMode` | String | No | `overwrite`, `append`, or `skip`. Default: `overwrite`. |
