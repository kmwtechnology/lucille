#!/bin/bash
set -e

# Validate that a config file has been specified and actually exists
if [ -z "$LUCILLE_CONF" ]; then
  echo "ERROR: LUCILLE_CONF environment variable must be set to the path of a Lucille config file."
  echo "Example: docker run --env LUCILLE_CONF=/lucille/conf/my-config.conf ..."
  exit 1
fi
if [ ! -f "$LUCILLE_CONF" ]; then
  echo "ERROR: LUCILLE_CONF points to a file that does not exist: ${LUCILLE_CONF}"
  echo "Did you forget to mount or COPY your config into the container?"
  exit 1
fi

# LUCILLE_ROLE selects which component this container runs:
#   runner  (default)  - com.kmwllc.lucille.core.Runner. Optionally driven by LUCILLE_OPTS (e.g. -usekafka)
#   worker             - com.kmwllc.lucille.core.Worker <pipeline>, requires LUCILLE_PIPELINE
#   indexer            - com.kmwllc.lucille.core.Indexer <pipeline>, requires LUCILLE_PIPELINE
LUCILLE_ROLE_NORMALIZED=$(echo "${LUCILLE_ROLE:-runner}" | tr '[:upper:]' '[:lower:]')
case "$LUCILLE_ROLE_NORMALIZED" in
  runner)
    if [ -n "$LUCILLE_PIPELINE" ]; then
      echo "WARN: LUCILLE_PIPELINE is set, but LUCILLE_ROLE=runner. It will not be used."
    fi
    MAIN_CLASS="com.kmwllc.lucille.core.Runner"
    MAIN_ARGS=(${LUCILLE_OPTS})
    ;;
  worker|indexer)
    if [ -z "$LUCILLE_PIPELINE" ]; then
      echo "ERROR: LUCILLE_PIPELINE must be set when LUCILLE_ROLE=${LUCILLE_ROLE_NORMALIZED}."
      exit 1
    fi
    if [ -n "$LUCILLE_OPTS" ]; then
      echo "WARN: LUCILLE_OPTS is set, but LUCILLE_ROLE=${LUCILLE_ROLE_NORMALIZED}. It will not be used."
    fi
    if [ "$LUCILLE_ROLE_NORMALIZED" = "worker" ]; then
      MAIN_CLASS="com.kmwllc.lucille.core.Worker"
    else
      MAIN_CLASS="com.kmwllc.lucille.core.Indexer"
    fi
    MAIN_ARGS=("$LUCILLE_PIPELINE")
    ;;
  *)
    echo "ERROR: unrecognized LUCILLE_ROLE '${LUCILLE_ROLE}'. Must be one of: runner, worker, indexer."
    exit 1
    ;;
esac

# JAVA_OPTS: JVM-level flags (heap, GC, etc.)
# exec replaces the shell so Java is PID 1 and receives signals (SIGTERM, SIGINT) directly
exec java \
  ${JAVA_OPTS} \
  -Dconfig.file="${LUCILLE_CONF}" \
  -cp '/lucille/lib/*' \
  "$MAIN_CLASS" \
  "${MAIN_ARGS[@]}"
