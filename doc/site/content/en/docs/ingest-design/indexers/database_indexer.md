---
title: Database Indexer
weight: 8
date: 2026-11-23
description: Configuration reference for the Database Indexer, which writes documents to a relational database with a SQL statement you write.
---

`com.kmwllc.lucille.indexer.DatabaseIndexer`

Config block: `indexer.database { ... }`

The Database Indexer runs a parameterized SQL statement for each document. The statement is yours, so database-specific behavior, such as an upsert or a cast to a JSON or vector type, is written in SQL. Any database with a JDBC driver works. The driver is not bundled: put its jar on the classpath, as for the [Database Connector]({{< relref "docs/ingest-design/connectors/database_connector" >}}).

| Parameter | Type | Required | Description |
|---|---|---|---|
| `driver` | String | Yes | JDBC driver class name, such as `org.postgresql.Driver`. |
| `connectionString` | String | Yes | JDBC connection URL. |
| `jdbcUser` | String | Yes | Username to access the database. |
| `jdbcPassword` | String | Yes | Password to access the database. Use an environment substitution such as `${PG_PASSWORD}`. |
| `sql` | String | Yes | Statement run for each document, with one `?` per entry in `params`. |
| `params` | List\<Object\> | Yes | How each `?` in `sql` is bound, in order. See [Parameters](#parameters). |
| `deleteSql` | String | No | Statement run for documents marked for deletion. Required when `indexer.deletionMarker` is set. |
| `deleteParams` | List\<Object\> | No | How each `?` in `deleteSql` is bound. Default: the document ID. |
| `validationSql` | String | No | Statement run at startup that must succeed, such as `SELECT ... LIMIT 0` on the target table, so a missing table or column fails the run early. |
| `retryableSqlStates` | List\<String\> | No | SQLState prefixes of transient failures, retried when `indexer.maxRetries` is set. Default: `["08", "40"]` (connection failures, deadlocks and serialization failures). |

```hocon
indexer {
  database {
    driver: "org.postgresql.Driver"
    connectionString: "jdbc:postgresql://localhost:5432/lucille"
    jdbcUser: "lucille"
    jdbcPassword: ${PG_PASSWORD}
    sql: """
      INSERT INTO documents (id, title, published_at)
      VALUES (?, ?, ?)
      ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, published_at = EXCLUDED.published_at
    """
    params: [
      { field: "id" }
      { field: "title" }
      { field: "published_at", format: "timestamp" }
    ]
  }
}
```

Mismatched or unused settings, such as a placeholder count that doesn't match `params`, fail at startup.

**Limitations:** the Database Indexer does not support `indexer.indexOverrideField`, `indexer.includeFields`, `indexer.excludeFields`, `indexer.deleteByQuery`, or `indexer.maxConcurrentBatches` greater than 1. Use `params` to choose what is written. Child documents are not written.

---

## Parameters

Each entry in `params` and `deleteParams` binds one `?`:

| Key | Type | Required | Description |
|---|---|---|---|
| `field` | String | Yes | Document field to bind. `id` binds the document's ID, or its `indexer.idOverrideField` value when that is set. |
| `format` | String | No | How the value is bound. See the table below. |
| `arrayType` | String | With `array` | The database's element type for an array, such as `text` or `float8`. |
| `sqlType` | String | No | A `java.sql.JDBCType` name, such as `INTEGER`, used to bind NULL when the field is missing. Only needed for drivers that can't infer the type of a NULL. |

| `format` | Binds |
|---|---|
| (none) | A single string, number or boolean. A multi-valued or object field fails that document. |
| `json` | Any value as JSON text. A list of floats becomes `[0.12,-0.03,...]`, which pgvector accepts. |
| `array` | A list, as a SQL array of `arrayType`. On databases without SQL arrays, such as MySQL, the document fails; use `json` instead. |
| `timestamp` | An ISO-8601 instant, such as `2024-01-01T00:00:00Z` (what `ParseDate` writes), as a UTC timestamp. |
| `bytes` | Binary data, or a base64 string. |

A value that doesn't suit its format fails that document. A missing field binds NULL; to keep the stored value instead, write the update as, for example, `title = COALESCE(EXCLUDED.title, documents.title)`.

PostgreSQL doesn't convert a string bound to a non-text column, and connectors such as the CSV file handler produce strings. Cast it in the statement (`?::int`), or add `stringtype=unspecified` to the connection string.

A `?` inside a string literal, quoted identifier or comment isn't a parameter. On PostgreSQL, write `??` for a literal `?`, such as the jsonb `?` operator.

---

## Writing the Statement

Write the statement as an upsert, so a retry or a re-run updates the row instead of failing on a duplicate key.

PostgreSQL:

```sql
INSERT INTO documents (id, title) VALUES (?, ?)
ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title
```

MySQL 8.0.19 and later (add `rewriteBatchedStatements=true` to the connection string for faster batches):

```sql
INSERT INTO documents (id, title) VALUES (?, ?) AS new
ON DUPLICATE KEY UPDATE title = new.title
```

SQL Server:

```sql
MERGE INTO documents AS t
USING (VALUES (?, ?)) AS s (id, title) ON t.id = s.id
WHEN MATCHED THEN UPDATE SET title = s.title
WHEN NOT MATCHED THEN INSERT (id, title) VALUES (s.id, s.title);
```

---

## How a Batch Is Sent

Each batch is sent in document order as one transaction, so a write and a delete of the same document apply in the order they arrived.

If the database rejects a document, for example for a constraint violation, only that document fails; the rest of the batch is committed. A failure with one of the `retryableSqlStates` rolls back the whole batch instead, and it is retried when `indexer.maxRetries` is set. Any other failure is not retried.

---

## Deletion

Set `indexer.deletionMarker` and `deleteSql`. A document marked for deletion runs `deleteSql` instead of `sql`:

```hocon
indexer {
  deletionMarker {
    field: "deleted"
    value: "true"
  }
  database {
    # ... connection settings, sql and params ...
    deleteSql: "DELETE FROM documents WHERE id = ?"
  }
}
```

---

## Connecting

The indexer connects at startup, not when a config is validated, and reconnects before a send if the connection was dropped. With `indexer.sendEnabled: false`, it doesn't connect at all.

---

## PostgreSQL and pgvector

The [lucille-postgres-vector-example](https://github.com/kmwtechnology/lucille/tree/main/lucille-examples/lucille-postgres-vector-example) indexes documents with embeddings into PostgreSQL with pgvector, and shows vector, hybrid and JSON queries against the result. Its statement:

```hocon
sql: """
  INSERT INTO documents (id, title, body, meta, tags, embedding)
  VALUES (?, ?, ?, ?::jsonb, ?, ?::vector)
  ON CONFLICT (id) DO UPDATE SET
    title = EXCLUDED.title, body = EXCLUDED.body, meta = EXCLUDED.meta,
    tags = EXCLUDED.tags, embedding = EXCLUDED.embedding, updated_at = now()
"""
params: [
  { field: "id" }
  { field: "title" }
  { field: "body" }
  { field: "meta" }
  { field: "tags", format: "array", arrayType: "text" }
  { field: "embeddings", format: "json" }
]
```

`?::jsonb` and `?::vector` cast the bound text to those types. A `vector(N)` column rejects an embedding of the wrong length, which fails that document.
