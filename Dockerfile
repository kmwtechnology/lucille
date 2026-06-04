FROM eclipse-temurin:21-jre

LABEL maintainer="kmwtechnology"
LABEL description="Lucille Search ETL base image"

# Create a standard working directory
WORKDIR /lucille

# Copy the built Lucille artifacts
COPY lucille-core/target/*.jar /lucille/lib/
COPY lucille-core/target/lib/ /lucille/lib/

COPY lucille-examples/lucille-rss-example/conf/single.conf /lucille/conf/single.conf

# Default config directory — downstream consumers mount or COPY configs here
RUN mkdir -p /lucille/conf

# non-root user
RUN groupadd -r lucille \
 && useradd -r -g lucille -d /lucille -s /usr/sbin/nologin lucille \
 && chown -R lucille:lucille /lucille

# Environment variables with sensible defaults
# LUCILLE_CONF: path to the Lucille config file (required)
# JAVA_OPTS: JVM flags such as heap size and GC settings
# LUCILLE_OPTS: Lucille Runner CLI flags (e.g. -local, -usekafka)
ENV LUCILLE_CONF=""
ENV LUCILLE_OPTS=""
ENV JAVA_OPTS="-Xms256m -Xmx1g"

# Copy the entrypoint script
COPY docker-entrypoint.sh /lucille/docker-entrypoint.sh
RUN chmod +x /lucille/docker-entrypoint.sh

USER lucille

ENTRYPOINT ["/lucille/docker-entrypoint.sh"]
