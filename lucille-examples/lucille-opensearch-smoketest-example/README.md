# OpenSearch Smoke Test

Runs Lucille against a real OpenSearch and checks what ends up in the index. It covers the bulk indexing path, delete by
id, and delete by query. Unit tests mock the OpenSearch client, so they can't show that these requests work against a
cluster.

This is a smoke test, not a demo. For the Enron ingest example, see `lucille-opensearch-ingest-example`.

## What it runs

`scripts/run-nightly.sh` starts a single-node OpenSearch 3.7.0 with `docker compose`, creates the index from
`mapping/index.json`, and then does three Lucille runs. Each run is followed by a verifier.

| Run | Input | Verifier | Expects in the index |
|---|---|---|---|
| 1. ingest | `data/ingest.csv`: 20 documents, 14 with `group=keep` and 6 with `group=obsolete` | `VerifyAfterIngest` | 20 documents, indexed in batches of 4 |
| 2. delete by id | `data/delete_by_id.csv`: document `3` with `deleted=true` | `VerifyAfterDeleteById` | document 3 gone, 19 documents left |
| 3. delete by query | `data/delete_by_query.csv`: `deleted=true` with `queryField=group`, `queryValue=obsolete` | `VerifyAfterDeleteByQuery` | no `group=obsolete` documents, 13 `keep` documents left |

All three runs use `conf/opensearch-smoke.conf`. The CSV's `id` column is the document id, so the verifiers know the
exact ids.

## Running it

Requirements: Docker with Compose v2, Java 21, and Maven.

```
mvn -pl lucille-examples/lucille-opensearch-smoketest-example -am package -DskipTests
./lucille-examples/lucille-opensearch-smoketest-example/scripts/run-nightly.sh
```

Or, from the example directory, `mvn verify -Pnightly`, which runs the same script in the `verify` phase.

The script exits non-zero if any run or verifier fails, and it always stops the stack (`docker compose down -v`).

### Settings

- `OPENSEARCH_HOST_PORT` (default `9201`): host port for the container. The default avoids a cluster already on `9200`.
- `OPENSEARCH_URL` (default `http://localhost:$OPENSEARCH_HOST_PORT`): where the runs and verifiers send requests. Set
  this to point at another cluster. The script then creates the index there, and fails if the index already exists. It
  never deletes an index.
- `OPENSEARCH_INDEX` (default `smoke-test`): the index name.

## Why these choices

- **Lucille runs on the host, and only OpenSearch is in Docker.** The runs use the same `target/lib` classpath as the
  other examples' `run_ingest.sh`.
- **Each run is a separate `Runner` process**, so the state between runs is only what is in OpenSearch.
- **Verifiers refresh the index before counting**, because indexing is near real time.
