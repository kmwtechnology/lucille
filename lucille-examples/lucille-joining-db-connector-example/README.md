# Joining Database Connector Example

This example shows how Lucille's `DatabaseConnector` can index relational data quickly by merge-joining multiple streaming result sets instead of running a single, wide SQL `JOIN`. That approach is especially effective when the tables you want to combine represent **one-to-many** relationships: one parent row (for example, an animal) and many related child rows (for example, meals).

A classic SQL join would expand every parent into one result row per child. That is expensive to stream, can explode memory on the database and the client, and leaves you with duplicated parent columns that you later have to collapse. A **merge join** avoids that: each query streams independently, both sides stay ordered by a shared join key, and Lucille walks the cursors together to assemble one document per parent.

## Why a merge join is a good fit for streaming ingest

The connector opens the primary query and each `otherSQLs` query as forward-only JDBC result sets. Because those result sets are already sorted by the join key, the connector never needs to buffer an entire table in memory or look up related rows by key. It only advances each cursor as far as the current parent id:

1. Read the next parent row from the primary result set.
2. Advance each secondary result set until its join key matches that parent.
3. Attach every consecutive matching child row as a child document.
4. Publish the parent and continue.

That is the same idea as a sort-merge join in a database engine: once both inputs are ordered, matching is a single linear pass. For large tables this is typically much faster than issuing a follow-up query per parent row, and it stays streaming-friendly for multi-million-row extracts.

## SQL must be ordered by the join key

The merge only works if **every** query is ordered by the same join key, in the same direction.

- The primary `sql` must `ORDER BY` the parent key (`idField` on the primary result set).
- Each statement in `otherSQLs` must `ORDER BY` its corresponding join column from `otherJoinFields`.
- The join columns must be comparable (typically integer keys). Null join keys are not supported.

If the result sets are not ordered, the connector will skip or mis-associate child rows: it only looks forward, never rewinds.

In this example the parent key is `animal.id` and the child key is `meal.animal_id`:

```sql
-- primary: one row per animal, ordered by the join key
select id, name, type from animal order by id

-- secondary: many meals per animal, ordered by the same key
select id as meal_id, animal_id, meal_type from meal order by animal_id
```

Those statements are wired together in `conf/joining-db-connector-example.conf` with `otherJoinFields: ["animal_id"]`, so the connector knows which column on the secondary result set to compare against the primary `id`.

## Collapsing one-to-many rows into multi-valued fields

Because the child result set is ordered by the join key, all meals for a given animal arrive as a contiguous run of rows. The connector attaches each of those rows as a child document on the parent.

The pipeline then uses `CollapseChildrenDocuments` to copy selected child fields onto the parent as **multi-valued** fields and drop the children:

```hocon
{
  name: collapseChildren
  class: com.kmwllc.lucille.stage.CollapseChildrenDocuments
  fieldsToCopy: ["meal_id", "meal_type"]
  dropChildren: true
}
```

After collapse, a parent document looks like a single search record with multi-valued related data, for example:

```json
{
  "id": "1",
  "name": "Matt",
  "type": "Human",
  "meal_id": [1, 2, 3],
  "meal_type": ["breakfast", "lunch", "dinner"]
}
```

You can add more statements to `otherSQLs` (and a matching entry in `otherJoinFields`) for additional one-to-many tables. Each extra result set is merge-joined the same way, as long as it is also ordered by the join key.

## Requirements

- A JDBC-accessible database with the tables you want to index (this example is written against MySQL).
- The matching JDBC driver on the classpath (this module already depends on `mysql-connector-j`).
- A destination index. The sample config writes to Solr at `http://localhost:8983/solr/dbtest`.

## Setup

1. Edit `conf/joining-db-connector-example.conf` and set `connectionString`, `jdbcUser`, and `jdbcPassword` for your database.
2. Point `solr.url` at your Solr collection, or change the indexer block if you want a different destination.
3. Confirm both SQL statements `ORDER BY` the join key, and that `otherJoinFields` names the child-side join column.

## Run

From this example directory, package the distribution and run the ingest script:

```bash
mvn package
./scripts/run_ingest.sh
```

The script starts Lucille with `conf/joining-db-connector-example.conf`. The connector streams the ordered result sets, merge-joins child rows onto each parent, collapses those children into multi-valued fields, and indexes the resulting documents.
