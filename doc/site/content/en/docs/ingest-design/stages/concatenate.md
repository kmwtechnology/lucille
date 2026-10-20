---
title: Concatenate
weight: 180
date: 2025-06-09
description: Builds a destination field from a format string with field values substituted into placeholders.
---

`com.kmwllc.lucille.stage.Concatenate`

Builds a value from `formatString` by replacing each `{field}` placeholder with the value of that field, and writes the result to `dest`. For example, `"{city}, {state}"` becomes `"Boston, MA"`. Only the first value of a multi-valued field is used. A placeholder for a missing field takes its value from `defaultInputs`; with no default, the placeholder is left as-is.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `dest` | String | Yes | Destination field. |
| `formatString` | String | Yes | Format string with `{field}` placeholders. |
| `defaultInputs` | Map\<String, String\> | No | Default values for fields missing from the Document. |
| `updateMode` | String | No | `overwrite`, `append`, or `skip`. Default: `overwrite`. |

```hocon
{ class: "com.kmwllc.lucille.stage.Concatenate", dest: "location", formatString: "{city}, {state}", defaultInputs: { state: "Unknown" } }
```
