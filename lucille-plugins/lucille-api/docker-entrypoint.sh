#!/bin/bash
set -e

if [ -z "$DROPWIZARD_CONF" ]; then
  echo "ERROR: DROPWIZARD_CONF must be set to the path of your API config file."
  echo "Example: docker run --env DROPWIZARD_CONF=conf/api.yml ..."
  exit 1
fi

# The API logs through log4j2 (APIApplication selects it). LUCILLE_LOG4J_CONF: log4j2 config file (defaults to the
#   editable conf/log4j2.xml). Setting it to empty falls back to the copy bundled in the jar.
if [ -n "$LUCILLE_LOG4J_CONF" ]; then
  LOG4J_OPT="-Dlog4j2.configurationFile=${LUCILLE_LOG4J_CONF}"
else
  LOG4J_OPT="-Dlog4j2.configurationFile=classpath:lucille-log4j2.xml"
fi

exec java \
  ${JAVA_OPTS} \
  "${LOG4J_OPT}" \
  -cp "lib/*" \
  com.kmwllc.lucille.api.APIApplication \
  server \
  ${DROPWIZARD_CONF}