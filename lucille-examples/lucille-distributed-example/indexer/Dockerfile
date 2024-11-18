FROM eclipse-temurin:17

RUN apt-get update && apt-get install curl -y && apt-get install jq -y

COPY target/ /target/
COPY conf/ /conf/
COPY indexer/scripts/ /scripts/

HEALTHCHECK --interval=5s --timeout=3s --retries=3 --start-period=5s \
  CMD curl -f 'http://solr:8983/solr/quickstart/admin/ping' | jq -e '.status == "OK"' || exit 1

# we specify a command that the image should run when it launches
ENTRYPOINT chmod +x /scripts/run.sh && /scripts/run.sh
