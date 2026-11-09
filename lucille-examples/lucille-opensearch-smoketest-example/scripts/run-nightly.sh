#!/bin/sh
set -e

# run from the example's root, so the compose file, data/, conf/ and target/ resolve
cd "$(dirname "$0")/.."

# 9201 by default, so the stack doesn't collide with an OpenSearch already on 9200 on a developer's machine
export OPENSEARCH_HOST_PORT="${OPENSEARCH_HOST_PORT:-9201}"
export OPENSEARCH_URL="${OPENSEARCH_URL:-http://localhost:$OPENSEARCH_HOST_PORT}"
export OPENSEARCH_INDEX="${OPENSEARCH_INDEX:-smoke-test}"

# stop OpenSearch however the script exits, including an unexpected failure under set -e.
# The script's exit code is kept, since the trap doesn't call exit.
trap 'docker compose down -v' EXIT

docker compose up -d --wait opensearch

# Create the index. The container is new on every run, so the index can't already exist. If OPENSEARCH_URL points at a
# cluster that already has it, this fails rather than overwriting it.
curl -fsS -X PUT "$OPENSEARCH_URL/$OPENSEARCH_INDEX" -H 'Content-Type: application/json' \
  -d @mapping/index.json >/dev/null

# Runs one Lucille run over the given CSV, then the verifier for the state that run should leave behind.
# The Runner exits non-zero if the run fails, and JUnitCore exits non-zero if any assertion fails. set -e stops on either.
run_stage() {
  csv="$1"
  verifier="$2"

  echo "== running $csv"
  INPUT_CSV="$csv" java -Dconfig.file=conf/opensearch-smoke.conf -cp 'target/lib/*' com.kmwllc.lucille.core.Runner

  echo "== verifying with $verifier"
  java -cp 'target/lib/*:target/test-classes' org.junit.runner.JUnitCore "$verifier"
}

run_stage data/ingest.csv com.kmwllc.lucille.examples.opensearchsmoke.VerifyAfterIngest
run_stage data/delete_by_id.csv com.kmwllc.lucille.examples.opensearchsmoke.VerifyAfterDeleteById
run_stage data/delete_by_query.csv com.kmwllc.lucille.examples.opensearchsmoke.VerifyAfterDeleteByQuery

echo "OpenSearch smoke test passed"
