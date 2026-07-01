#!/bin/bash
set -e

# Retry Solr collection creation up to 10 times.
# HTTP 200 = created successfully.
# HTTP 400 typically means the collection already exists (acceptable for re-runs).
# Any other status is treated as a transient error and retried.
for i in $(seq 1 10); do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    'http://solr:8983/solr/admin/collections?action=CREATE&name=quickstart&numShards=1&collection.configName=_default')
  if [ "$STATUS" = "200" ] || [ "$STATUS" = "400" ]; then
    echo "Collection ready (HTTP $STATUS)"
    break
  fi
  echo "Attempt $i: collection creation returned HTTP $STATUS, retrying in 3s..."
  sleep 3
  if [ "$i" = "10" ]; then
    echo "ERROR: Failed to create Solr collection after 10 attempts"
    exit 1
  fi
done

java -Dconfig.file=/conf/main.conf -cp '/target/lib/*' com.kmwllc.lucille.core.Indexer simple_pipeline
