# This Dockerfile defines an image that runs the Lucille Admin API using the latest OpenJDK 17
# with a Lucille configuration file that can be specified via an environment variable.
#
# To build an image using this Dockerfile you must first:
#   clone the lucille repo
#   run "mvn clean install" from the top-level lucille repo
#
# Once you have completed the build process locally, you should cd into the top level of the lucille-admin module
# and run the following command to build the image:
#
#   docker build -t lucille-admin-api .
#
# Now that you have built the image, you can launch a container running that image.
# The Admin API will start when the image launches.
# You need to tell Lucille which configuration file it should use.
# You can use a config file inside the image (this Dockerfile copies conf/* into the image)
# You can also use a config file on your local filesystem.
#
# To launch the container with the example config file that's inside the image, run this:
#   docker run --env LUCILLE_CONF=conf/simple-config.conf -p 8080:8080 lucille-admin-api
# In the above command, "conf/simple-config.conf" is a path inside the image
#
# To use a config file on your local filesystem, assuming that the file lives at <absolute-path-on-local-fs>/conf/my_file.conf
# run this:
#
#   docker run --env LUCILLE_CONF=/mnt/conf/my_file.conf -v <absolute-path-on-local-fs>/conf:/mnt/conf -p 8080:8080 lucille-admin-api
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
# we use a base image with the latest OpenJDK 17
FROM eclipse-temurin:17

# we copy local build artifacts and configuration into the image that we're building
COPY conf/ conf/
COPY target/ target/

# Set the necessary environment variables
ENV LUCILLE_CONF="conf/simple-config.conf"
ENV DROPWIZARD_CONF="conf/admin.yml"

# Expose port 8080 so we can make requests to the API
EXPOSE 8080

ENTRYPOINT java -Dconfig.file=${LUCILLE_CONF} -jar target/lucille-admin.jar server ${DROPWIZARD_CONF}