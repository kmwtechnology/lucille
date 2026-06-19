# This Dockerfile defines an image that runs the Lucille Admin API using the latest OpenJDK 17
# with a Lucille configuration file that can be specified via an environment variable.
#
# To build an image using this Dockerfile you must first:
#   clone the lucille repo
#   run "mvn clean install" from the top-level lucille repo
#
# Once you have completed the build process locally, you should cd into the top level of the lucille-api module
# and run the following command to build the image:
#
#   docker build -t lucille-api .
#
# Now that you have built the image, you can launch a container running that image.
# The Admin API will start when the image launches.
# You need to tell Lucille which configuration file it should use.
# You can use a config file inside the image (this Dockerfile copies conf/* into the image)
# You can also use a config file on your local filesystem.
#
# To launch the container with the example config file that's inside the image, run this:
#   docker run --env DROPWIZARD_CONF=conf/api.yml -p 8080:8080 lucille-api
# In the above command, "conf/api.yml" is a path inside the image
#
# To use a config file on your local filesystem, assuming that the file lives at <absolute-path-on-local-fs>/conf/my_file.conf
# run this:
#
#   docker run --env DROPWIZARD_CONF=/mnt/conf/api_conf.yml -v <absolute-path-on-local-fs>/conf:/mnt/conf -p 8080:8080 lucille-api
#
# By default, Lucille log output is written both to STDOUT and to file in the a "log" directory that is created where Lucille is run
# Since log output goes to STDOUT you will see it in the container's output.
# If you want to view it after the container terminates you can run "docker ps -a" to find the ID of the terminated container.
# Once you have the ID, you can then run "docker logs <ID>" to see the output.
# If you would like the container to write logs to your local filesystem, you can add a parameter like the following one
# when you launch the container. Output written to "/log" inside the container will then be go to your chosen
# local directory which is specified here as <absolute-path-on-local-fs>/mylog. The "mylog" directory will be created if
# it does not already exist:
#
#       -v <absolute-path-on-local-fs>/mylog:/log
#
# What follows is the main content of this Dockerfile:
#
# we use a base image with the latest OpenJDK 21
FROM eclipse-temurin:21-jre

LABEL maintainer="kmwtechnology"
LABEL description="Lucille API base image"

WORKDIR /lucille-api

RUN apt-get update && apt-get upgrade -y

# we copy local build artifacts and configuration into the image that we're building
COPY conf/ /lucille-api/conf/
COPY target/ /lucille-api/target/

# non-root user
RUN groupadd -r lucille \
 && useradd -r -g lucille -d /lucille-api -s /usr/sbin/nologin lucille \
 && chown -R lucille:lucille /lucille-api

# Set the necessary environment variables
ENV JAVA_OPTS=""
ENV DROPWIZARD_CONF="conf/api.yml"

# Expose port 8080 so we can make requests to the API
EXPOSE 8080

COPY docker-entrypoint.sh /lucille-api/docker-entrypoint.sh
RUN chmod +x /lucille-api/docker-entrypoint.sh

USER lucille

ENTRYPOINT ["/lucille-api/docker-entrypoint.sh"]