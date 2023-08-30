# This Dockerfile defines an image that runs Lucille in local mode using the latest OpenJDK 11
# with a Lucille configuration file that can be specified via an environment variable.
#
# To build an image using this Dockerfile you must first:
#   clone the lucille repo
#   run "mvn clean install" inside the lucille-core module
#   run "mvn clean install" inside the lucille-examples/lucille-file-to-file-example module
#
# Once you have completed the build process locally, you should cd into the top level of the lucille-file-to-file example
# and run the following command to build the image:
#
#   docker build -t lucille-example1 .
#
# Now that you have built the image, you can launch a container running that image.
# Lucille will start when the image launches.
# You need to tell Lucille which configuration file it should use.
# You can use a config file inside the image (this Dockerfile copies conf/file-to-file-example.conf into the image)
# You can also use a config file on your local filesystem.
#
# To launch the container with the example config file that's inside the image, run this:
#   docker run --env CONF=conf/file-to-file-example.conf -it lucille-example1
# In the above command, "conf/file-to-file-example.conf" is a path inside the image
#
# To use a config file on your local filesystem, assuming that the file lives at <absolute-path-on-local-fs>/conf/my_file.conf
# run this:
#
#   docker run --env CONF=/mnt/conf/my_file.conf -v <absolute-path-on-local-fs>/conf:/mnt/conf -it lucille-example1
#
# This example generates an output file at /output/dest.csv inside the container.
# If you want this output file to be written to your local disk, you need to map
# the /output directory inside the container to a local directory by adding an additional -v argument as follows.
# Assume you want the output to go to /home/tester/tmp
#
#       -v /home/tester/tmp/:/output
#
# The full command might look like this:
#
# docker run --env CONF=/mnt/conf/my_file.conf -v /home/tester/conf:/mnt/conf -v /home/tester/tmp:/output -it lucille-example1
#
# What follows is the main content of this Dockerfile:
#
# we use a base image with the latest OpenJDK 11
FROM eclipse-temurin:11

# we copy local build artifacts into the image that we're building
COPY scripts/ /scripts/
COPY target/ /target/
COPY conf/ /conf/

# we specify a command that the image should run when it launches
# in this case, we are running lucille in local mode
# the path to the config file is specified via a CONF environment variable that must be provided when launching the container
ENV CONF=""
ENTRYPOINT java -Dconfig.file=${CONF} -cp 'target/lib/*' com.kmwllc.lucille.core.Runner -local