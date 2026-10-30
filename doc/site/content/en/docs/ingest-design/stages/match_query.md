---
title: MatchQuery
weight: 400
date: 2025-06-09
description: Runs a set of named Lucene queries against document fields and records the names of the queries that match.
---

`com.kmwllc.lucille.stage.MatchQuery`

Runs a set of named Lucene queries against text fields of each document, using a Lucene Monitor held in memory, and adds the name of each matching query to `matchedQueriesField`. Queries use Lucene query syntax and are analyzed with the `StandardAnalyzer`. Terms without a field prefix are matched against the first field in `fields`. Only the first value of a multi-valued field is used, and missing fields are skipped. If no query matches, `matchedQueriesField` is not set. Matched names are appended to any existing values in `matchedQueriesField`. A query that fails to parse fails the stage at startup. The Lucene Monitor and analyzer are closed when the stage stops.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fields` | List\<String\> | Yes | Document fields to match queries against. Must not be empty. |
| `queries` | List\<Map\<String, String\>\> | Yes | List of objects, each mapping a query name to a Lucene query string. Must not be empty. |
| `matchedQueriesField` | String | Yes | Field that receives the names of the matching queries. |

```hocon
{
  class: "com.kmwllc.lucille.stage.MatchQuery"
  fields: ["title", "body"]
  queries: [
    { solar: "solar OR photovoltaic" },
    { wind: "body:\"wind turbine\"" }
  ]
  matchedQueriesField: "topics"
}
```
