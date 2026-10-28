---
title: QueryDatabase
weight: 500
date: 2025-06-09
description: Executes a JDBC prepared statement using document field values and merges the result onto the document.
---

`com.kmwllc.lucille.stage.QueryDatabase`

Executes a JDBC prepared statement using document field values as parameters and merges the result onto the document. Useful for per-document database enrichment (e.g., joining a lookup table for each document mid-pipeline).

The connection and prepared statement are created at startup. For each document, the first value of each field in `keyFields` is bound to the matching `?` in `sql`. If a key field is missing, the document fails. For every row returned, each column in `fieldMapping` is added to its mapped field. Values are added to any existing values, so multiple rows (or an existing field) produce a multi-valued field. SQL `NULL` values and unsupported column types are skipped. A `fieldMapping` column that is not in the result set fails the document.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `driver` | String | Yes | JDBC driver class name. |
| `connectionString` | String | Yes | JDBC connection URL. |
| `jdbcUser` | String | Yes | Database username. |
| `jdbcPassword` | String | Yes | Database password. |
| `sql` | String | Yes | SQL query with `?` placeholders for parameters. |
| `keyFields` | List\<String\> | Yes | Document field names whose values are substituted for `?` in the SQL, in order. |
| `inputTypes` | List\<String\> | Yes | Type of each key field: `String`, `Integer`, `Long`, `Double`, `Boolean`, or `Date` (case-sensitive). Must match `keyFields` length, or the stage fails at startup. |
| `fieldMapping` | Map\<String, String\> | Yes | Maps result-set column names to document field names. |
| `connectionRetries` | Integer | No | Number of times to retry connecting at startup after the first attempt fails. Values less than `1` fall back to the default. Default: `1`. |
| `connectionRetryPause` | Integer | No | Milliseconds to wait between connection attempts. Values less than `1` fall back to the default. Default: `10000`. |

```hocon
{
  class: "com.kmwllc.lucille.stage.QueryDatabase"
  driver: "org.postgresql.Driver"
  connectionString: "jdbc:postgresql://localhost:5432/shop"
  jdbcUser: "lucille"
  jdbcPassword: ${DB_PASSWORD}
  sql: "SELECT name, price FROM products WHERE sku = ?"
  keyFields: ["sku"]
  inputTypes: ["String"]
  fieldMapping: { name: "product_name", price: "product_price" }
}
```
