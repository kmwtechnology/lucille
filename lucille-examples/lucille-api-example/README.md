# Lucille API Example

Runs the `lucille-api` Docker image (the same Dockerfile published as `kmwtechnology/lucille-api`) and verifies it end to end over HTTP.

## How to run the example

1. Run `mvn clean install -DskipTests -Pshaded-plugin-jars` at the top level directory of lucille. `-Pshaded-plugin-jars` ensures the `lucille-api-plugin.jar` is built.
2. Run `mvn verify -Pnightly` in the `lucille-api-example` directory. (`scripts/run-nightly.sh` can also be run directly, but needs `mvn package` to have been run in this directory first.)

### What it checks

The `api` service has a healthcheck on its readiness probe, and the `verifier` container starts once it passes. It runs `VerifyIngestResult` (`src/test/java/com/kmwllc/lucille/api/VerifyIngestResult.java`) against the `api` container. It's not run as part of normal Lucille builds. The build also copies the jars it needs into `target/verifier-lib`, which `verifier/Dockerfile` puts into the image. 

1. `/v1/readyz` returns 200. This is checked first by the `api` service's healthcheck in `docker-compose.yml`, which the `verifier` waits on, and then again by the test.
2. `/v1/livez` returns 200.
3. Basic auth is enforced: `/v1/run` returns 401 without credentials and 200 with them (auth is enabled in `conf/api.yml`).
4. `/v1/config-info/connector-list` returns connectors.
5. An invalid HOCON upload to `/v1/config` returns 400.
6. `conf/run_a.conf` and `conf/run_b.conf` are uploaded and started at the same time. Each uses a SequenceConnector to generate 500 docs, with its own doc ID prefix, a `run` field, and an output CSV on a volume shared with the verifier. 
Both runs must succeed with 500 docs, and each CSV must contain exactly its own run's 500 docs, with every stage applied, and nothing from the other run.
The test does *not* enforce that these runs are executed concurrently.

`scripts/run-nightly.sh` fails early (printing the API's logs) if the API never becomes healthy. Otherwise, it waits for `verifier` to exit, prints its logs (and the API's logs on failure), tears the stack down, and exits with the verifier's exit code.
