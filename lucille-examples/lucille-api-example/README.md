# Lucille API Example

Runs the `lucille-api` Docker image (the same Dockerfile published as `kmwtechnology/lucille-api`) and verifies it end to end over HTTP.

## How to run the example

1. Run `mvn clean install -DskipTests -Pshaded-plugin-jars` at the top level directory of lucille. The profile is required: it builds `lucille-plugins/lucille-api/target/lucille-api-plugin.jar`, which the image runs.
2. Run `mvn verify -Pnightly` in the `lucille-api-example` directory. (`scripts/run-nightly.sh` can also be run directly, but needs `mvn package` to have been run in this directory first.)

### What it checks

The `verifier` container runs `verifier/verification.sh`, which waits for the API's readiness probe and then runs `VerifyIngestResult` (`src/test/java/com/kmwllc/lucille/api/VerifyIngestResult.java`) against the `api` container. It's a JUnit test, but its name doesn't match surefire's default includes, so a normal build only compiles it. The build also copies the jars it needs into `target/verifier-lib`, which `verifier/Dockerfile` puts into the image. `ConfigValidationTest`, like in the other examples, validates the configs in `conf/` during the normal build.

1. `/v1/readyz` and `/v1/livez` return 200.
2. Basic auth. is enforced: `/v1/run` returns 401 without credentials and 200 with them (auth is enabled in `conf/api.yml`).
3. `/v1/config-info/connector-list` returns connectors.
4. An invalid HOCON upload to `/v1/config` returns 400.
5. `conf/run_a.conf` and `conf/run_b.conf` are uploaded and started at the same time. Each uses a SequenceConnector to generate 500 docs, with its own doc ID prefix, a `run` field, and an output CSV on a volume shared with the verifier. The pipeline runs six simple stages (SetStaticValues, CopyFields, ReplacePatterns, Concatenate, RenameFields, DeleteFields). Both runs must succeed with 500 docs, and each CSV must contain exactly its own run's 500 docs, with every stage applied, and nothing from the other run.

`scripts/run-nightly.sh` waits for `verifier` to exit, prints its logs (and the API's logs on failure), tears the stack down, and exits with the verifier's exit code.
