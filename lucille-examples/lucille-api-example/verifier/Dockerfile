# Image for the verifier container, which runs IngestDriver and then the verification tests against the api
# container. Built from the example's root (see docker-compose.yml) so it can copy the compiled test classes and
# the jars they need, which the example's build puts in target/. The command that runs them is the verifier
# service's entrypoint in docker-compose.yml.
FROM eclipse-temurin:21-jre

COPY target/test-classes/ /verifier/classes/
COPY target/verifier-lib/ /verifier/lib/

# java uses this when no -cp is given, so the entrypoint can run classes by name
ENV CLASSPATH="/verifier/classes:/verifier/lib/*"
