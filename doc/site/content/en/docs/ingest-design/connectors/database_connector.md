---
title: Database Connector
weight: 2
date: 2025-06-09
description: A Connector that reads rows from a JDBC-compatible database and publishes each row as a Lucille Document.
---

[Source Code](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/jdbc/DatabaseConnector.java)

`com.kmwllc.lucille.connector.jdbc.DatabaseConnector`

The `DatabaseConnector` runs a `SELECT` statement against a JDBC-compatible relational database and publishes each row as a Lucille Document. Column labels (lowercased) become field names on the Document. Columns whose value is `NULL` are not added. Character types become strings; `TINYINT`, `SMALLINT` and `INTEGER` become integers; `BIGINT` becomes a long; `DECIMAL` and `NUMERIC` values with no fractional part become longs when they fit, and other values become doubles (so they can lose precision); `DOUBLE` becomes a double; `FLOAT` and `REAL` become floats; `BOOLEAN` and `BIT` become booleans; `DATE` and `TIMESTAMP` become instants, reading the stored date and time as UTC regardless of the JVM time zone; `TIME` becomes a string such as `10:15:30`; binary types become byte arrays. A column of any other SQL type (for example `TIMESTAMP WITH TIME ZONE`, `ARRAY`, JSON or a `UUID` that the driver reports as binary) fails the run; cast it in the SQL or list it in `ignoreColumns`.

## Basic Configuration

```hocon
connectors: [
  {
    name: "db-connector"
    class: "com.kmwllc.lucille.connector.jdbc.DatabaseConnector"
    pipeline: "my-pipeline"

    driver: "org.postgresql.Driver"
    connectionString: "jdbc:postgresql://localhost:5432/mydb"
    jdbcUser: "username"
    jdbcPassword: ${?DB_PASSWORD}
    sql: "SELECT id, title, body, published_at FROM articles WHERE active = true"
    idField: "id"
  }
]
```

## Configuration Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `driver` | String | Yes | JDBC driver class name. The driver JAR must be on the classpath. The driver is loaded when the connector executes, so a missing driver fails the run rather than startup. |
| `connectionString` | String | Yes | JDBC connection URL. The connection is opened when the connector executes, not at startup. |
| `jdbcUser` | String | Yes | Database username. Can be `""`. |
| `jdbcPassword` | String | Yes | Database password. Can be `""`. Use `${?VAR}` for environment variable substitution. |
| `sql` | String | Yes | `SELECT` statement to execute. All returned rows are published as Documents. |
| `idField` | String | Yes | Column whose value becomes the Document ID, matched case-insensitively. If the query does not return this column, the run fails. |
| `fetchSize` | Integer | No | JDBC fetch size hint, applied to the main query and to `otherSQLs`. For MySQL, set to `Integer.MIN_VALUE` (i.e., `-2147483648`) to avoid buffering the full result set in memory. If omitted, the driver's default is used. |
| `preSQL` | String | No | A SQL statement (INSERT, DELETE, UPDATE, or DDL) executed once before the main query. Useful for creating temp tables, acquiring locks, or seeding data. |
| `postSQL` | String | No | A SQL statement executed once after all rows have been published. Useful for cleanup, releasing locks, or writing completion markers. |
| `otherSQLs` | List\<String\> | No | Additional `SELECT` queries whose matching rows are attached to each Document as child Documents. See [Multi-Query Joins](#multi-query-joins). |
| `otherJoinFields` | List\<String\> | No | Join column for each entry in `otherSQLs`, in the same order. Required when `otherSQLs` is set: omitting it fails at startup. A list shorter than `otherSQLs` fails the run when rows are processed. Ignored without `otherSQLs`. |
| `ignoreColumns` | List\<String\> | No | Column names from the main query to skip when populating Documents, matched case-insensitively. Not applied to `otherSQLs` columns. |
| `connectionRetries` | Integer | No | Number of retries after a failed connection attempt, so up to `connectionRetries + 1` attempts are made for each connection. Values below 1 are treated as `1`. Default: `1`. |
| `connectionRetryPause` | Integer | No | Milliseconds to wait between connection attempts. Values below 1 are treated as the default. Default: `10000`. |

The Document ID is `docIdPrefix` (if set) followed by the `idField` value. A row whose `idField` value is `NULL` or blank is skipped and logged, and the number of skipped rows is logged at the end. The `idField` column is also added as a field, unless it is named `id`. Any other column whose lowercased name is a reserved field name (such as `id` when it is not the `idField`, or `run_id`) fails the run unless it is listed in `ignoreColumns`.

## Pre and Post SQL

Use `preSQL` and `postSQL` to run setup and teardown logic that must happen before and after the main query:

```hocon
{
  name: "db-connector"
  class: "com.kmwllc.lucille.connector.jdbc.DatabaseConnector"
  pipeline: "my-pipeline"

  driver: "org.postgresql.Driver"
  connectionString: "jdbc:postgresql://localhost:5432/mydb"
  jdbcUser: ${?DB_USER}
  jdbcPassword: ${?DB_PASSWORD}

  preSQL: "CREATE TEMP TABLE export_snapshot AS SELECT * FROM articles WHERE active = true"
  sql: "SELECT id, title, body FROM export_snapshot ORDER BY id"
  postSQL: "DROP TABLE IF EXISTS export_snapshot"
  idField: "id"
}
```

If `preSQL` or `postSQL` fails, the error is logged and the run continues. `postSQL` is skipped if the main query or publishing fails. Connections are always closed in `close()`.

## Multi-Query Joins

`otherSQLs` allows you to enrich primary rows with data from additional queries at read time, without a SQL JOIN. Each row of a secondary query is matched to the primary row whose `idField` value equals the secondary row's `otherJoinFields` column. The main query must be ordered by `idField`, and each secondary query by its join column, because the result sets are merged in a single forward pass. Join values are compared with Java `compareTo`, so the `idField` column and each join column must return mutually comparable Java types (for example numbers, strings or dates); a mismatched type fails the run. The `idField` values must be unique, and an out-of-order or duplicate primary key, or an out-of-order secondary key, fails the run rather than silently dropping child rows. Secondary rows whose join value is `NULL` are skipped. Each secondary query runs on its own connection, and all of them are executed one after another before the first primary row is read.

```hocon
{
  sql: "SELECT id, title FROM articles ORDER BY id"
  idField: "id"
  otherSQLs: ["SELECT article_id, tag_name FROM article_tags ORDER BY article_id"]
  otherJoinFields: ["article_id"]
}
```

For each primary row, every matching row from `otherSQLs` is attached to the Document as a child Document, with all of the secondary query's lowercased column names (including the join column) becoming fields on the child and `NULL` values skipped. Child values use the same type mapping as the main query, so an unsupported column type fails the run, as does a child column named `id` or another reserved field. Child Document IDs have the form `<parentId>_<n>`, where `n` counts the parent's children across all `otherSQLs` starting at `0`, so they are unique. The values are not merged into multi-valued fields on the parent, and whether children reach the index depends on the Indexer.

To flatten the children into multi-valued fields, add the [`CollapseChildrenDocuments`]({{< relref "docs/ingest-design/stages/collapse_children_documents" >}}) Stage to the pipeline:

```hocon
{
  name: "collapse-children"
  class: "com.kmwllc.lucille.stage.CollapseChildrenDocuments"
  fieldsToCopy: ["tag_name"]
  dropChildren: true
}
```

Each article Document then carries a multi-valued `tag_name` field and no child Documents.

## Incremental Ingest

The `DatabaseConnector` does not maintain internal state. For incremental ingest (only rows modified since the last run), filter at the SQL level:

```sql
SELECT id, title, updated_at FROM articles
WHERE updated_at > TIMESTAMP '2025-01-01 00:00:00'
ORDER BY updated_at ASC
```

Store the high-water mark externally (e.g., in a config file, environment variable, or the database itself) and substitute it via HOCON variable substitution.

## MySQL Streaming

For large MySQL tables, set `fetchSize` to avoid loading the entire result set into memory:

```hocon
{
  driver: "com.mysql.cj.jdbc.Driver"
  connectionString: "jdbc:mysql://localhost:3306/mydb?useCursorFetch=true"
  fetchSize: -2147483648
  sql: "SELECT id, title FROM large_table ORDER BY id"
  idField: "id"
}
```

## Common JDBC Drivers

| Database | Driver Class | Maven Artifact |
|---|---|---|
| PostgreSQL | `org.postgresql.Driver` | `org.postgresql:postgresql` |
| MySQL | `com.mysql.cj.jdbc.Driver` | `com.mysql:mysql-connector-j` |
| SQL Server | `com.microsoft.sqlserver.jdbc.SQLServerDriver` | `com.microsoft.sqlserver:mssql-jdbc` |
| SQLite | `org.sqlite.JDBC` | `org.xerial:sqlite-jdbc` |
| Apache Derby | `org.apache.derby.iapi.jdbc.AutoloadedDriver` | `org.apache.derby:derby` |
| H2 | `org.h2.Driver` | `com.h2database:h2` |

## Integration with QueryDatabase Stage

For per-document database enrichment (joining a lookup table for each document mid-pipeline, rather than reading a source table), use the [`QueryDatabase`]({{< relref "docs/ingest-design/stages/query_database" >}}) Stage.
