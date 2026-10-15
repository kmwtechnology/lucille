#!/bin/sh

# Runner reports success once the Connector finishes publishing and completion events are
# received, but Worker/Indexer startup can lag behind that (JVM boot, Kafka consumer group
# join), so Solr may not have all documents indexed yet by the time this container starts.
# Poll until Solr reports the expected doc count instead of asserting on a single point-in-time
# snapshot; if it never converges, fall through and let the JUnit test report the mismatch.
EXPECTED_DOCS=6
MAX_ATTEMPTS=60
SLEEP_SECS=5

attempt=1
while [ "$attempt" -le "$MAX_ATTEMPTS" ]; do
  curl -sf 'http://solr:8983/solr/quickstart/update?commit=true' > /dev/null
  curl -sf 'http://solr:8983/solr/quickstart/query?q=*:*' > /output/dest.json
  NUM_FOUND=$(jq '.response.numFound' /output/dest.json)

  if [ "$NUM_FOUND" = "$EXPECTED_DOCS" ]; then
    echo "Found ${NUM_FOUND} docs after ${attempt} attempt(s)."
    break
  fi

  echo "Attempt ${attempt}/${MAX_ATTEMPTS}: found ${NUM_FOUND:-0} of ${EXPECTED_DOCS} expected docs. Retrying in ${SLEEP_SECS}s..."
  attempt=$((attempt + 1))
  sleep "$SLEEP_SECS"
done

mvn test -DfailIfNoTests=true
