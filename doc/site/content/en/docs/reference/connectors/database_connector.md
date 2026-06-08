---
title: Database Connector
weight: 2
date: 2025-06-09
description: A Connector that reads rows from a JDBC-compatible database and publishes each row as a Lucille Document.
---

[Source Code](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/jdbc/DatabaseConnector.java)

The `DatabaseConnector` reads rows from any JDBC-compatible relational database and publishes each row as a Lucille Document. Column names become field names on the Document.

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
| `driver` | String | Yes | JDBC driver class name. The driver JAR must be on the classpath. |
| `connectionString` | String | Yes | JDBC connection URL. |
| `jdbcUser` | String | No | Database username. |
| `jdbcPassword` | String | No | Database password. Use `${?VAR}` for environment variable substitution. |
| `sql` | String | Yes | SELECT statement to execute. All returned rows are published as Documents. |
| `idField` | String | No | Column whose value becomes the Document ID. If omitted, a UUID is generated per row. |
| `docIdPrefix` | String | No | Prefix prepended to every Document ID. |
| `fetchSize` | Integer | No | JDBC fetch size hint for streaming large result sets. For MySQL, set to `Integer.MIN_VALUE` (i.e., `-2147483648`) to avoid buffering the full result set in memory. |
| `preSQL` | String | No | A SQL statement (INSERT, DELETE, UPDATE, or DDL) executed once before the main query. Useful for creating temp tables, acquiring locks, or seeding data. |
| `postSQL` | String | No | A SQL statement executed once after the main query completes successfully. Useful for cleanup, releasing locks, or writing completion markers. |
| `otherSQLs` | List\<String\> | No | Additional SELECT queries to JOIN onto the primary result. Each query must return rows ordered by its join key. |
| `otherJoinFields` | List\<String\> | No | Join fields parallel to `otherSQLs`. Required when `otherSQLs` is specified. Must be integer-valued columns. |
| `ignoreColumns` | List\<String\> | No | Column names to skip when populating Documents. |
| `connectionRetries` | Integer | 1 | Number of connection retry attempts on failure. |
| `connectionRetryPause` | Integer | 10000 | Milliseconds to wait between connection retries. |

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

`postSQL` runs only if `preSQL` and the main query both succeed. If the connector throws, `postSQL` is skipped but `close()` is always called.

## Multi-Query Joins

`otherSQLs` allows you to enrich primary rows with data from additional queries at read time, without a SQL JOIN. Each secondary query must be ordered by its join key (which must be an integer column matching a column in the primary query).

```hocon
{
  sql: "SELECT id, title FROM articles ORDER BY id"
  idField: "id"
  otherSQLs: ["SELECT article_id, tag_name FROM article_tags ORDER BY article_id"]
  otherJoinFields: ["article_id"]
}
```

For each primary row, the connector merges matching rows from `otherSQLs` as multi-valued fields onto the Document.

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

For per-document database enrichment (joining a lookup table for each document mid-pipeline, rather than reading a source table), use the [`QueryDatabase`]({{< relref "docs/reference/stages/stages_reference" >}}) Stage.
