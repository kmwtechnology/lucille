#!/bin/bash
set -e

exec java \
  ${JAVA_OPTS} \
  -Dconfig.file="${LUCILLE_CONF}" \
  -jar target/lucille-api-plugin.jar \
  server \
  ${DROPWIZARD_CONF}