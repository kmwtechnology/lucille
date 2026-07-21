## How to run the example

1. Run ```mvn clean install -DskipTests``` at the top level directory of lucille to build the project.
2. Run ```mvn verify -Pnightly``` in the ```lucille-distributed-example``` directory to run the distributed example.

### What the mvn verify -Pnightly command does

1. It runs the nightly profile defined in the pom.xml, which execs `scripts/run-nightly.sh`.
2. That script starts the full stack in the background (`docker compose up -d --build`). Lucille runs in distributed mode with 2 connectors executed sequentially.
3. It then blocks until the `verifier` container exits. The Runner, Worker, and Indexer are either long-running or exit on their own once the run completes, so waiting on `verifier` specifically (rather than `docker compose up --abort-on-container-exit`, which aborts on whichever container exits *first*) avoids a race that could tear the stack down before verification runs.
4. Once `verifier` exits, the script reads its actual exit code via `docker inspect`, tears down the stack (`docker compose down`), and exits with that code — so `mvn verify` fails if and only if verification failed.

**Local runs:** After each run, remove containers to avoid a stale state, otherwise the test will error.

### A Note on Dockerfiles
The `runner`, `worker`, and `indexer` services all build and run from the same Dockerfile
in the root of the project — the same one published as the `lucille-core` image. Which
process it starts (`Runner`, `Worker`, or `Indexer`) is selected with the `LUCILLE_ROLE`
and `LUCILLE_PIPELINE` environment variables set on each service in `docker-compose.yml`.
Running the example this way means the nightly smoketest exercises the actual published
image, not a bespoke one.

Solr collection creation and post-run verification (querying Solr and running the
`VerifyIngestResult` JUnit test) aren't things Lucille itself needs to do, so that logic
lives in two separate, throwaway images instead of being baked into `lucille-core`:
the `solr-init` service (a stock `curlimages/curl` image) creates the Solr collection
before the Indexer starts, and the `verifier` service (built from `runner/Dockerfile`)
runs after the Runner completes, verifying the results.