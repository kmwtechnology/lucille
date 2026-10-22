#!/bin/sh

# The api container reports "started" as soon as its JVM launches, well before Dropwizard is serving
# requests. Poll the readiness probe until it returns 200 before running the tests; if it never does,
# fall through and let VerifyIngestResult report the failure.
MAX_ATTEMPTS=60
SLEEP_SECS=2

attempt=1
while [ "$attempt" -le "$MAX_ATTEMPTS" ]; do
  if curl -sf -o /dev/null 'http://api:8080/v1/readyz'; then
    echo "API ready after ${attempt} attempt(s)."
    break
  fi

  echo "Attempt ${attempt}/${MAX_ATTEMPTS}: API not ready yet. Retrying in ${SLEEP_SECS}s..."
  attempt=$((attempt + 1))
  sleep "$SLEEP_SECS"
done

exec java -cp '/verifier/classes:/verifier/lib/*' org.junit.runner.JUnitCore com.kmwllc.lucille.api.VerifyIngestResult
