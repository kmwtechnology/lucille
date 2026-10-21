#!/bin/sh
# Exercises the lucille-api container over HTTP: health probes, auth, config-info, and a full
# config upload -> run -> completed-run round trip. Exits non-zero on the first failed check.
set -eu

API="http://api:8080"
AUTH="user:example-password"
EXPECTED_DOCS=3
MAX_ATTEMPTS=60
SLEEP_SECS=2

fail() {
  echo "FAIL: $*"
  exit 1
}

# prints the HTTP status code of a request; extra args are passed through to curl
status_of() {
  curl -s -o /dev/null -w '%{http_code}' "$@"
}

echo "Waiting for the API to become ready..."
attempt=1
until [ "$(status_of "$API/v1/readyz")" = "200" ]; do
  [ "$attempt" -ge "$MAX_ATTEMPTS" ] && fail "API never became ready at $API/v1/readyz"
  attempt=$((attempt + 1))
  sleep "$SLEEP_SECS"
done
echo "API is ready."

[ "$(status_of "$API/v1/livez")" = "200" ] || fail "liveness probe did not return 200"

# auth is enabled in conf/api.yml, so protected endpoints must reject requests without credentials
[ "$(status_of "$API/v1/run")" = "401" ] || fail "GET /v1/run without credentials should return 401"
[ "$(status_of -u "$AUTH" "$API/v1/run")" = "200" ] || fail "GET /v1/run with credentials should return 200"

# component scanning must work inside the shaded jar
NUM_CONNECTORS=$(curl -sf -u "$AUTH" "$API/v1/config-info/connector-list" | jq 'length')
[ "$NUM_CONNECTORS" -gt 0 ] || fail "connector-list returned no connectors"
echo "connector-list returned ${NUM_CONNECTORS} connectors."

# malformed configs are rejected
[ "$(status_of -u "$AUTH" -X POST "$API/v1/config" -H 'Content-Type: application/hocon' -d '{ not valid')" = "400" ] \
  || fail "uploading an invalid config should return 400"

CONFIG_ID=$(curl -sf -u "$AUTH" -X POST "$API/v1/config" \
  -H 'Content-Type: application/hocon' --data-binary @/pipeline.conf | jq -r '.configId')
[ -n "$CONFIG_ID" ] && [ "$CONFIG_ID" != "null" ] || fail "config upload did not return a configId"
echo "Uploaded config ${CONFIG_ID}."

RUN_ID=$(curl -sf -u "$AUTH" -X POST "$API/v1/run" \
  -H 'Content-Type: application/json' -d "{\"configId\":\"${CONFIG_ID}\"}" | jq -r '.runId')
[ -n "$RUN_ID" ] && [ "$RUN_ID" != "null" ] || fail "starting a run did not return a runId"
echo "Started run ${RUN_ID}."

attempt=1
while :; do
  DETAILS=$(curl -sf -u "$AUTH" "$API/v1/run/${RUN_ID}")
  [ "$(echo "$DETAILS" | jq -r '.endTime')" != "null" ] && break
  [ "$attempt" -ge "$MAX_ATTEMPTS" ] && fail "run ${RUN_ID} did not complete. Last details: ${DETAILS}"
  attempt=$((attempt + 1))
  sleep "$SLEEP_SECS"
done

echo "Run details: ${DETAILS}"
[ "$(echo "$DETAILS" | jq -r '.runResult.status')" = "true" ] || fail "run ${RUN_ID} did not succeed"
echo "$DETAILS" | jq -r '.runResult.message' | grep -q "${EXPECTED_DOCS} docs succeeded" \
  || fail "expected ${EXPECTED_DOCS} docs to succeed"

echo "PASS: all lucille-api checks succeeded."
