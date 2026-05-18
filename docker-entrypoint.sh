#!/bin/bash
set -e

# Validate that a config file has been specified
if [ -z "$LUCILLE_CONF" ]; then
  echo "ERROR: LUCILLE_CONF environment variable must be set to the path of a Lucille config file."
  echo "Example: docker run --env LUCILLE_CONF=/lucille/conf/my-config.conf ..."
  exit 1
fi

# Build and execute the java command
# JAVA_OPTS: JVM-level flags (heap, GC, etc.)
# LUCILLE_OPTS: Lucille Runner CLI flags (e.g. -local, -usekafka)
# exec replaces the shell so Java is PID 1 and receives signals (SIGTERM, SIGINT) directly
exec java \
  ${JAVA_OPTS} \
  -Dconfig.file="${LUCILLE_CONF}" \
  -cp '/lucille/lib/*' \
  com.kmwllc.lucille.core.Runner \
  ${LUCILLE_OPTS}
