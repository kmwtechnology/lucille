FROM eclipse-temurin:17

RUN apt-get update && apt-get install curl procps -y

COPY target/ /target/
COPY conf/ /conf/
COPY indexer/scripts/ /scripts/

HEALTHCHECK --interval=5s --timeout=3s --retries=10 --start-period=15s \
  CMD pgrep -f "com.kmwllc.lucille.core.Indexer" || exit 1

# we specify a command that the image should run when it launches
ENTRYPOINT chmod +x /scripts/run.sh && /scripts/run.sh
