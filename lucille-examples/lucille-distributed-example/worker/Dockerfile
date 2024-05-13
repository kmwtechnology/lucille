FROM eclipse-temurin:17

COPY target/ /target/
COPY conf/ /conf/
COPY worker/scripts/ /scripts/

# we specify a command that the image should run when it launches
ENTRYPOINT chmod +x /scripts/run.sh && /scripts/run.sh
