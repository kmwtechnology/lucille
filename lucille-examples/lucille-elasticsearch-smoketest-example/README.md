# Elasticsearch Smoke Test

Runs Lucille against a real Elasticsearch and checks what ends up in the index. It covers the bulk indexing path, delete
by id, and delete by query. Unit tests mock the Elasticsearch client, so they can't show that these requests work against
a cluster.

This is the Elasticsearch counterpart of `lucille-opensearch-smoketest-example`. The two run the same three stages over
the same data, so a difference between them points at the indexer rather than the test.

## What it runs

`scripts/run-nightly.sh` starts a single-node Elasticsearch 9.0.0 with `docker compose`, creates the index from
`mapping/index.json`, and then does three Lucille runs. Each run is followed by a verifier.

| Run | Input | Verifier | Expects in the index |
|---|---|---|---|
| 1. ingest | `data/ingest.csv`: 20 documents, 14 with `group=keep` and 6 with `group=obsolete` | `VerifyAfterIngest` | 20 documents, indexed in batches of 4 |
| 2. delete by id | `data/delete_by_id.csv`: document `3` with `deleted=true` | `VerifyAfterDeleteById` | document 3 gone, 19 documents left |
| 3. delete by query | `data/delete_by_query.csv`: `deleted=true` with `queryField=group`, `queryValue=obsolete` | `VerifyAfterDeleteByQuery` | no `group=obsolete` documents, 13 `keep` documents left |

All three runs use `conf/elasticsearch-smoke.conf`. The CSV's `id` column is the document id, so the verifiers know the
exact ids.

## Running it

Requirements: Docker with Compose v2, Java 21, and Maven.

```
mvn -pl lucille-examples/lucille-elasticsearch-smoketest-example -am package -DskipTests
./lucille-examples/lucille-elasticsearch-smoketest-example/scripts/run-nightly.sh
```

Or, from the example directory, `mvn verify -Pnightly`, which runs the same script in the `verify` phase.

The script exits non-zero if any run or verifier fails, and it always stops the stack (`docker compose down -v`).

### Settings

- `ELASTICSEARCH_HOST_PORT` (default `9202`): host port for the container. The default avoids the ports used by a local
  OpenSearch or Elasticsearch.
- `ELASTICSEARCH_URL` (default `http://localhost:$ELASTICSEARCH_HOST_PORT`): where the runs and verifiers send requests.
  Set this to point at another cluster. The script then creates the index there, and fails if the index already exists.
  It never deletes an index.
- `ELASTICSEARCH_INDEX` (default `smoke-test`): the index name.

## Why these choices

- **Security is off in the container** (`xpack.security.enabled=false`), so the runs use plain HTTP. Against a secured
  cluster, set `ELASTICSEARCH_URL` with credentials and `acceptInvalidCert` as the indexer config needs.
- **Lucille runs on the host, and only Elasticsearch is in Docker**, with the same `target/lib` classpath the other
  examples use.
- **Each run is a separate `Runner` process**, so the state between runs is only what is in Elasticsearch.
- **Verifiers refresh the index before counting**, because indexing is near real time.
