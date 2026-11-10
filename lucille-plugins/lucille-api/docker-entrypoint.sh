#!/bin/bash
set -e

if [ -z "$DROPWIZARD_CONF" ]; then
  echo "ERROR: DROPWIZARD_CONF must be set to the path of your API config file."
  echo "Example: docker run --env DROPWIZARD_CONF=conf/api.yml ..."
  exit 1
fi

exec java \
  ${JAVA_OPTS} \
  -cp "lib/*" \
  com.kmwllc.lucille.api.APIApplication \
  server \
  ${DROPWIZARD_CONF}