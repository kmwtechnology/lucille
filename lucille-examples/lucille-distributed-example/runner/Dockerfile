FROM ubuntu
RUN apt-get upgrade
RUN apt-get update && apt-get install curl -y
RUN apt-get install lsof bc openjdk-17-jre-headless -y --fix-missing

# we copy local build artifacts into the image that we're building
COPY target/ /target/
COPY conf/ /conf/
COPY runner/scripts/ /scripts/
RUN mkdir /output/

# we specify a command that the image should run when it launches
ENTRYPOINT chmod +x /scripts/run.sh && /scripts/run.sh
