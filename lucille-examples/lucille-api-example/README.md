# Lucille API Example

Runs the `lucille-api` Docker image (the same Dockerfile published as `kmwtechnology/lucille-api`) and verifies it end to end over HTTP.

## How to run the example

1. Run `mvn clean install -DskipTests -Pshaded-plugin-jars` at the top level directory of lucille. The profile is required: it builds `lucille-plugins/lucille-api/target/lucille-api-plugin.jar`, which the image runs.
2. Run `mvn verify -Pnightly` in the `lucille-api-example` directory (or run `scripts/run-nightly.sh` directly).

### What it checks

The `verifier` container (`verifier/verify.sh`) runs against the `api` container and fails on the first check that doesn't pass:

1. `/v1/readyz` and `/v1/livez` return 200.
2. Basic auth is enforced: `/v1/run` returns 401 without credentials and 200 with them (auth is enabled in `conf/api.yml`).
3. `/v1/config-info/connector-list` returns connectors, so component scanning works inside the shaded jar.
4. An invalid HOCON upload to `/v1/config` returns 400.
5. `verifier/pipeline.conf` is uploaded, a run is started with it, and the run completes successfully with all 3 docs from `conf/source.csv` processed.

`scripts/run-nightly.sh` waits for `verifier` to exit, prints its logs (and the API's logs on failure), tears the stack down, and exits with the verifier's exit code.

### Poking at the API manually

`docker compose up -d --build api` starts only the API on `localhost:8080`. Requests need `-u user:example-password`, for example `curl -u user:example-password localhost:8080/v1/run`. Run `docker compose down` when you're done.
