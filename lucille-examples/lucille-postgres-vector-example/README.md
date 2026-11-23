# PostgreSQL + pgvector Example

This example indexes documents into PostgreSQL with the `JDBCIndexer`, storing an embedding per document in a
[pgvector](https://github.com/pgvector/pgvector) column next to the text, tags and JSON metadata. Postgres then serves
vector similarity search, full-text search, and both combined, from one table.

The `JDBCIndexer` runs a SQL statement you write for each document. Everything Postgres-specific is in that statement:
an `ON CONFLICT` upsert, and casts to `jsonb` and `vector`. The indexer itself has no Postgres code, so the same approach
works for any database with a JDBC driver.

## What's in the example

- `conf/documents.csv`: eight short documents, each with a 4-dimensional embedding. The dimensions roughly stand for
  animals, space, cooking and finance, so similarity results are easy to check by eye. The last row is marked
  `deleted: true`.
- `conf/schema.sql`: the `documents` table, with a generated `tsvector` column for full-text search, an HNSW index on the
  embedding and a GIN index on the `tsvector`.
- `conf/postgres-vector-example.conf`: the pipeline and indexer config.

## Running it

Run these commands from the `lucille-postgres-vector-example` directory.

Start Postgres with pgvector, and once it is accepting connections (a few seconds), create the table:

```bash
docker run -d --name lucille-postgres-example -e POSTGRES_USER=lucille -e POSTGRES_PASSWORD=lucille -e POSTGRES_DB=lucille -p 5432:5432 pgvector/pgvector:pg16
docker exec -i lucille-postgres-example psql -U lucille -d lucille < conf/schema.sql
```

Build the example together with the Lucille modules it depends on, so it uses the `lucille-core` from this checkout
rather than an older one in your local Maven repository, then run the ingest:

```bash
(cd ../.. && mvn package -DskipTests -pl lucille-examples/lucille-postgres-vector-example -am)
./scripts/run_ingest.sh
```

The connection defaults to `localhost:5432`, user and password `lucille`. Override them with `PG_CONNECTION_STRING`,
`PG_USER` and `PG_PASSWORD`.

Running the ingest again updates the rows instead of failing on duplicate ids, because the statement is an upsert.

## How the config maps documents to rows

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
deleteSql: "DELETE FROM documents WHERE id = ?"
```

- Each entry in `params` binds one `?`, in order. The number of `?` must match `params`, or the run fails at startup.
- `meta` is JSON text in the CSV, so it is bound as a string and cast with `?::jsonb`.
- `tags` is a list after the `SplitFieldValues` stage. `format: "array"` binds it as a Postgres `text[]`.
- `embeddings` is a list of floats. `format: "json"` writes it as `[0.92,0.05,...]`, which is pgvector's text format, and
  `?::vector` casts it. The `vector(4)` column rejects an embedding of the wrong length.
- Documents marked by `indexer.deletionMarker` run `deleteSql` instead.

A document without a field writes NULL to its column. To keep the stored value instead, write the update as
`title = COALESCE(EXCLUDED.title, documents.title)`.

## Querying

Nearest neighbors to an "animals" vector:

```sql
SELECT id, title, 1 - (embedding <=> '[0.9,0.05,0.05,0]') AS score
  FROM documents
 ORDER BY embedding <=> '[0.9,0.05,0.05,0]'
 LIMIT 3;
```

Hybrid search, filtering by full text and ranking by vector similarity:

```sql
SELECT id, title, 1 - (embedding <=> '[0.1,0,0.9,0]') AS score
  FROM documents
 WHERE body_tsv @@ to_tsquery('english', 'honey | bread')
 ORDER BY score DESC;
```

JSON metadata:

```sql
SELECT meta->>'source' AS source, count(*) FROM documents GROUP BY 1;
```

## Using real embeddings

The sample data carries its embeddings so the example runs without an API key. In a real pipeline, replace the
`ParseFloats` stage with an embedding stage, for example:

```hocon
{
  name: "embed"
  class: "com.kmwllc.lucille.stage.OpenAIEmbed"
  source: "body"
  dest: "embeddings"
  apiKey: ${OPENAI_API_KEY}
  embedDocument: true
  embedChildren: false
  modelName: "text-embedding-3-small"
}
```

and change the column to the model's dimension, `vector(1536)` for `text-embedding-3-small`.
