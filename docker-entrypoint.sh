#!/bin/bash
set -e

# LUCILLE_ROLE selects which component this container runs. Every component main installs the shutdown hook that
# handles SIGTERM and SIGINT:
#   runner  (default)  - com.kmwllc.lucille.core.Runner ${LUCILLE_OPTS}        (e.g. -distributed, -external)
#   worker             - com.kmwllc.lucille.core.Worker ${LUCILLE_PIPELINE}    requires LUCILLE_PIPELINE
#   indexer            - com.kmwllc.lucille.core.Indexer ${LUCILLE_PIPELINE}   requires LUCILLE_PIPELINE
#   worker-indexer     - com.kmwllc.lucille.core.WorkerIndexer ${LUCILLE_PIPELINE}   requires LUCILLE_PIPELINE
#   api                - com.kmwllc.lucille.api.APIApplication server ${DROPWIZARD_CONF}   lucille-all image only.
#                        Runs the Admin API (and UI at /admin). Lucille configs are POSTed to it, so LUCILLE_CONF is unused.
LUCILLE_ROLE_NORMALIZED=$(echo "${LUCILLE_ROLE:-runner}" | tr '[:upper:]' '[:lower:]')

if [ "$LUCILLE_ROLE_NORMALIZED" = "api" ]; then
  if [ -z "$DROPWIZARD_CONF" ]; then
    echo "ERROR: DROPWIZARD_CONF must be set to the path of an API config file when LUCILLE_ROLE=api."
    echo "Example: docker run --env LUCILLE_ROLE=api --env DROPWIZARD_CONF=/lucille/conf/api.yml ..."
    exit 1
  fi
  if [ ! -f "$DROPWIZARD_CONF" ]; then
    echo "ERROR: DROPWIZARD_CONF points to a file that does not exist: ${DROPWIZARD_CONF}"
    exit 1
  fi
  if ! ls /lucille/lib/lucille-api-*.jar > /dev/null 2>&1; then
    echo "ERROR: LUCILLE_ROLE=api needs the Admin API on the classpath. Use the lucille-all image."
    exit 1
  fi
else
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
fi

case "$LUCILLE_ROLE_NORMALIZED" in
  api)
    if [ -n "$LUCILLE_OPTS" ] || [ -n "$LUCILLE_PIPELINE" ]; then
      echo "WARN: LUCILLE_OPTS / LUCILLE_PIPELINE are set, but LUCILLE_ROLE=api. They will not be used."
    fi
    MAIN_CLASS="com.kmwllc.lucille.api.APIApplication"
    MAIN_ARGS=(server "$DROPWIZARD_CONF")
    ;;
  runner)
    if [ -n "$LUCILLE_PIPELINE" ]; then
      echo "WARN: LUCILLE_PIPELINE is set, but LUCILLE_ROLE=runner. It will not be used."
    fi
    MAIN_CLASS="com.kmwllc.lucille.core.Runner"
    MAIN_ARGS=(${LUCILLE_OPTS})
    ;;
  worker|indexer|worker-indexer)
    if [ -z "$LUCILLE_PIPELINE" ]; then
      echo "ERROR: LUCILLE_PIPELINE must be set when LUCILLE_ROLE=${LUCILLE_ROLE_NORMALIZED}."
      exit 1
    fi
    if [ -n "$LUCILLE_OPTS" ]; then
      echo "WARN: LUCILLE_OPTS is set, but LUCILLE_ROLE=${LUCILLE_ROLE_NORMALIZED}. It will not be used."
    fi
    case "$LUCILLE_ROLE_NORMALIZED" in
      worker) MAIN_CLASS="com.kmwllc.lucille.core.Worker" ;;
      indexer) MAIN_CLASS="com.kmwllc.lucille.core.Indexer" ;;
      worker-indexer) MAIN_CLASS="com.kmwllc.lucille.core.WorkerIndexer" ;;
    esac
    MAIN_ARGS=("$LUCILLE_PIPELINE")
    ;;
  *)
    echo "ERROR: unrecognized LUCILLE_ROLE '${LUCILLE_ROLE}'. Must be one of: runner, worker, indexer, worker-indexer, api."
    exit 1
    ;;
esac

# JAVA_OPTS: JVM-level flags (heap, GC, etc.)
# LUCILLE_OPTS: Lucille Runner CLI flags (e.g. -distributed, -external)
# LUCILLE_LOG4J_CONF: log4j2 config file (defaults to the editable /lucille/conf/log4j2.xml). lucille-core
#   ships no log4j2 config of its own ; we point log4j2 at the external file here. Setting it to empty
#   falls back to the non-auto-discovered copy bundled in lucille-logging.jar.
# exec replaces the shell so Java is PID 1 and receives signals (SIGTERM, SIGINT) directly
if [ -n "$LUCILLE_LOG4J_CONF" ]; then
  LOG4J_OPT="-Dlog4j2.configurationFile=${LUCILLE_LOG4J_CONF}"
else
  LOG4J_OPT="-Dlog4j2.configurationFile=classpath:lucille-log4j2.xml"
fi

# The lucille-all image also carries Logback (Dropwizard depends on it), so two SLF4J providers are on the classpath
# and SLF4J would otherwise pick whichever it finds first. Pin log4j2 for every role. It comes before JAVA_OPTS so a
# -Dslf4j.provider there still wins (the last -D for a property takes effect).
SLF4J_OPT="-Dslf4j.provider=org.apache.logging.slf4j.SLF4JServiceProvider"

# config.file is Typesafe Config's system property for the Lucille config; the api role has none (configs are POSTed).
CONFIG_OPTS=()
if [ "$LUCILLE_ROLE_NORMALIZED" != "api" ]; then
  CONFIG_OPTS=(-Dconfig.file="${LUCILLE_CONF}")
fi

exec java \
  "${SLF4J_OPT}" \
  ${JAVA_OPTS} \
  "${LOG4J_OPT}" \
  "${CONFIG_OPTS[@]}" \
  -cp '/lucille/lib/*' \
  "$MAIN_CLASS" \
  "${MAIN_ARGS[@]}"
