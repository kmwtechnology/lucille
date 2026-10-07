---
title: QueryDatabase
weight: 500
date: 2025-06-09
description: Executes a JDBC prepared statement using document field values and merges the result onto the document.
---

`com.kmwllc.lucille.stage.QueryDatabase`

Executes a JDBC prepared statement using document field values as parameters and merges the result onto the document. Useful for per-document database enrichment (e.g., joining a lookup table for each document mid-pipeline).

| Parameter | Type | Required | Description |
|---|---|---|---|
| `driver` | String | Yes | JDBC driver class name. |
| `connectionString` | String | Yes | JDBC connection URL. |
| `jdbcUser` | String | Yes | Database username. |
| `jdbcPassword` | String | Yes | Database password. |
| `sql` | String | No | SQL query with `?` placeholders for parameters. |
| `keyFields` | List\<String\> | Yes | Document field names whose values are substituted for `?` in the SQL, in order. |
| `inputTypes` | List\<String\> | Yes | JDBC types for each key field (e.g., `"STRING"`, `"INT"`, `"LONG"`). Must match `keyFields` length. |
| `fieldMapping` | Map\<String, String\> | Yes | Maps result-set column names to document field names. |
