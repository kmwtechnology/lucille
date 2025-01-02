# Use an official OpenJDK 21 base image
FROM openjdk:21-jdk-slim

# Install necessary dependencies for Maven and Git
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    maven \
    git \
    && rm -rf /var/lib/apt/lists/*


# Clone the specific branch of the repository
RUN git clone --branch LC-579-s3-text-chunking-example https://github.com/supertick/lucille.git /lucille

# Set working directory to the cloned repository
WORKDIR /lucille
RUN git pull

# Build the project using Maven
RUN mvn clean install --quiet -DskipTests

# Set the working directory for the starting command
WORKDIR /lucille/lucille-plugins/lucille-api/

# Expose port 8080
EXPOSE 8080

# Define the default command to run the application
CMD ["java", "-Dconfig.file=conf/simple-config.conf", "-cp", "/lucille/lucille-core/target/lucille.jar:/lucille/lucille-examples/lucille-s3-ingest-example/conf/tika-config.xml:/lucille/lucille-examples/lucille-s3-ingest-example/target/lib/*:/lucille/lucille-s3-ingest-example-0.4.2-SNAPSHOT.jar:/lucille/lucille-plugins/lucille-api/target/lucille-api-plugin.jar", "com.kmwllc.lucille.APIApplication", "server", "conf/api.yml"]
