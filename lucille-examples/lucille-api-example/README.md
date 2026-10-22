# Lucille API Example

Runs the `lucille-api` Docker image (the same Dockerfile published as `kmwtechnology/lucille-api`) and verifies it end to end over HTTP.

## How to run the example

1. Run `mvn clean install -DskipTests -Pshaded-plugin-jars` at the top level directory of lucille. `-Pshaded-plugin-jars` ensures the `lucille-api-plugin.jar` is built.
2. Run `mvn verify -Pnightly` in the `lucille-api-example` directory. (`scripts/run-nightly.sh` can also be run directly, but needs `mvn package` to have been run in this directory first.)

### What it checks

The `api` service has a healthcheck on its readiness probe, so `/v1/readyz` must return 200 before anything else starts. Then the `verifier` container starts, and runs the following, in order. The code is in `src/test/java/com/kmwllc/lucille/api/`, and none of it runs as part of normal Lucille builds.

**`IngestDriver`** drives the ingest through the API. It uploads `conf/run_a.conf` as HOCON and `conf/run_b.json` as JSON, so both of `/v1/config`'s content types are covered, and starts both runs at the same time. Each config uses a SequenceConnector to generate 500 docs, with its own doc ID prefix, a `run` field, and an output CSV on a volume shared with the verifier. Both runs must succeed with 500 docs, with distinct run IDs. If an upload or run fails, it fails, and the JUnit classes below don't run. It reports whether the runs overlapped in time, but doesn't fail if they didn't.

**Then two JUnit classes run.**

`VerifyApiEndpoints` checks the API's endpoints:
1. `/v1/readyz` and `/v1/livez` return 200.
2. Basic auth is enforced: `/v1/run` returns 401 without credentials and 200 with them (auth is enabled in `conf/api.yml`).
3. `/v1/config-info/connector-list` returns connectors.
4. An invalid HOCON upload to `/v1/config` returns 400.

`VerifyIngestResult` checks the runs' output. Each run's CSV must contain exactly its own run's 500 docs, with every pipeline stage applied, and nothing from the other run.

`scripts/run-nightly.sh` fails early (printing the API's logs) if the API never becomes healthy. Otherwise, it waits for `verifier` to exit, prints its logs (and the API's logs on failure), tears the stack down, and exits with the verifier's exit code.
