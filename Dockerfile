# Single multi-stage Dockerfile for both Lucille images:
#
#   lucille       (core only)        -> docker build -t lucille:latest .              (default; builds the `core` stage)
#   lucille-all   (all plugins)      -> docker build --target all -t lucille-all:latest .
#
# Run `mvn clean install` first so each stage's target/lib exists. The `core` stage is intentionally
# the LAST stage in this file so that a plain `docker build .` (no --target) produces the minimal core
# image; building the larger all-plugins image is an explicit opt-in via `--target all`.

# ---------------------------------------------------------------------------------------------------
# Shared base: everything the two images have in common (JRE, working dir, config dir, user, env,
# entrypoint). The only thing the image stages add is which set of jars lands on /lucille/lib.
# ---------------------------------------------------------------------------------------------------
FROM eclipse-temurin:21-jre AS base

LABEL maintainer="kmwtechnology"

# Create a standard working directory
WORKDIR /lucille

# remove pebble due to CVE with go. unused.
RUN apt-get update && apt-get upgrade -y && rm -f /usr/bin/pebble /bin/pebble

# Default config directory — downstream consumers mount or COPY configs here
RUN mkdir -p /lucille/conf

# non-root user
RUN groupadd -r lucille \
 && useradd -r -g lucille -d /lucille -s /usr/sbin/nologin lucille \
 && chown -R lucille:lucille /lucille

# Environment variables with sensible defaults
# LUCILLE_CONF: path to the Lucille config file (required)
# LUCILLE_ROLE: which Lucille component to run: runner, worker, indexer or worker-indexer. Defaults to runner (which is the component to use for local runs)
# LUCILLE_PIPELINE: pipeline name to process; use only (required) when LUCILLE_ROLE is worker, indexer or worker-indexer
# JAVA_OPTS: JVM flags such as heap size and GC settings
# LUCILLE_OPTS: Runner CLI flags, used only when LUCILLE_ROLE=runner (e.g. -distributed, -external)
ENV LUCILLE_CONF=""
ENV LUCILLE_ROLE="runner"
ENV LUCILLE_PIPELINE=""
ENV LUCILLE_OPTS=""
ENV JAVA_OPTS=""

# Copy the entrypoint script
COPY docker-entrypoint.sh /lucille/docker-entrypoint.sh
RUN chmod +x /lucille/docker-entrypoint.sh

USER lucille

ENTRYPOINT ["/lucille/docker-entrypoint.sh"]

# ---------------------------------------------------------------------------------------------------
# all: lucille-core + every component plugin (Solr, OpenSearch, Elasticsearch, Pinecone, Weaviate,
# Parquet, Tika, OCR, Jlama, entity-extraction, video). Larger image, lowest barrier to entry.
#
# Note: this image is large (lucille-video bundles native ffmpeg/OpenCV for every platform) and is
# tied to one CPU architecture, because lucille-jlama resolves its native library to the architecture
# this image is BUILT on. Build it on (or cross-build it for) the architecture you will run it on, or
# the Jlama stage will fail to load its native code.
#
# lucille-all's copy-dependencies build places lucille-core, every plugin jar, and all of their
# transitive runtime dependencies into target/lib. Keeping each jar separate (rather than shading)
# lets the per-plugin META-INF/services/IndexerTypeProvider files merge naturally via ServiceLoader.
# ---------------------------------------------------------------------------------------------------
FROM base AS all
LABEL description="Lucille Search ETL batteries-included image (lucille-core + all component plugins)"
COPY lucille-all/target/lib/ /lucille/lib/

# ---------------------------------------------------------------------------------------------------
# core: lucille-core only. The minimal image, and the default build target (last stage in this file).
# Unlike the all-plugins lib, lucille-core's target/lib holds only core's dependencies, so the core
# jar (lucille.jar, which carries the Runner main class) is copied in separately.
# ---------------------------------------------------------------------------------------------------
FROM base AS core
LABEL description="Lucille Search ETL base image (lucille-core only)"
COPY lucille-core/target/lucille.jar /lucille/lib/
COPY lucille-core/target/lib/ /lucille/lib/
