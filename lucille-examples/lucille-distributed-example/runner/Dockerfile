FROM eclipse-temurin:17

# we copy local build artifacts into the image that we're building
COPY target/ /target/
COPY conf/ /conf/
COPY runner/scripts/ /scripts/

# we specify a command that the image should run when it launches
ENTRYPOINT chmod +x /scripts/run.sh && /scripts/run.sh
